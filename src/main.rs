use clap::Parser;
use nix::sys::uio::{pread, pwrite};
use std::env;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::thread;
use std::{fs::File, path::PathBuf};

#[derive(Parser)]
#[command(name = "Parallel copy")]
#[command(author = "Matt S. <matt.storey@netvalue.nz>")]
#[command(version = "0.1.0")]
#[command(about = "Threaded copy of files", long_about = None)]
struct Cli {
    #[arg(short)]
    in_file: PathBuf,
    #[arg(short)]
    out_file: PathBuf,
    #[arg(default_value_t = 32)]
    threads: u8,
    verify: Option<bool>,
}

fn time_as_double() -> f64 {
    // High precision time.
    let now = std::time::SystemTime::now();
    let since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    since_epoch.as_secs_f64()
}

fn verify_copy(
    file1: &str,
    file2: &str,
    file_size: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    eprintln!(
        "Verifying '{}' and '{}' are the same after copy. Size {}",
        file1, file2, file_size
    );
    let mut in1 = File::open(file1)?;
    let mut in2 = File::open(file2)?;

    let buffer_size: usize = 10 * 1024 * 1024; // 10Mb
    let mut buffer1 = vec![0; buffer_size];
    let mut buffer2 = vec![0; buffer_size];

    for step in (0..file_size).step_by(buffer_size) {
        let bytes_read_from_file1 = in1.read(&mut buffer1)?;
        let bytes_read_from_file2 = in2.read(&mut buffer2)?;

        if bytes_read_from_file1 == bytes_read_from_file2 {
            if &buffer1[..bytes_read_from_file1] != &buffer2[..bytes_read_from_file2] {
                return Err(format!("File differ at range starting {} bytes", step).into());
            }
        } else {
            eprintln!("*warning* uneven reads during varificaion");
        }
    }

    Ok("Verified files are identical".into())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <infile> <outfile> [num_threads]", args[0]);
        std::process::exit(1);
    }
    let verify = true; // TODO: add arg parsing

    let inf = &args[1];
    let ouf = &args[2];
    let num_threads = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);

    let infile = File::open(inf).unwrap();
    let infile_size = infile.metadata().unwrap().len() as usize;
    eprintln!(
        "Copying. Infile size: {}, with threads {}",
        infile_size, num_threads
    );
    let infile = Arc::new(infile);
    let outfile = File::create(ouf).unwrap();
    outfile.set_len(infile_size as u64).unwrap();
    // allocate_space(&outfile, infile_size)?;
    // fallocate(file.as_raw_fd(), FallocateFlags::empty(), 0, length as i64)

    // fn allocate_space(file: &File, length: u64) -> Result<(), nix::Error> {
    //     fallocate(file.as_raw_fd(), FallocateFlags::empty(), 0, length as i64)
    // }

    let outfile = Arc::new(outfile);

    let mut threads = Vec::new();
    let slice = infile_size / num_threads;
    let start_time = time_as_double();

    let one_tenth_file_size = infile_size / 10;
    let processed_bytes = Arc::new(Mutex::new(0usize));

    for thrd_num in 0..num_threads {
        let infile = Arc::clone(&infile);
        let outfile = Arc::clone(&outfile);
        let processed_bytes = Arc::clone(&processed_bytes);

        let t = thread::spawn(move || {
            let mut buffer = vec![0; 1024 * 1024];
            let mut pos = thrd_num * slice;
            let mut local_processed = 0usize;

            while pos < (thrd_num + 1) * slice {
                let size_bytes_read = pread(&*infile, &mut buffer, pos as i64).unwrap();
                if size_bytes_read > 0 {
                    pwrite(&*outfile, &buffer[..size_bytes_read], pos as i64).unwrap();
                    pos += size_bytes_read;
                    local_processed += size_bytes_read;
                } else {
                    break;
                }
                if local_processed >= one_tenth_file_size {
                    let mut total_processed = processed_bytes.lock().unwrap();
                    *total_processed += local_processed;
                    eprint!(
                        "\r[progress] {:.1}%",
                        (*total_processed as f64 / infile_size as f64) * 100.0
                    );
                    local_processed = 0;
                }
            }
        });

        threads.push(t);
    }

    for t in threads {
        t.join().unwrap();
    }
    let finish_time = time_as_double();
    eprintln!(
        "\n*info* Finished! {} bytes written in {:.1} seconds = {:.3} Gbits/s",
        infile_size,
        finish_time - start_time,
        infile_size as f64 / (finish_time - start_time) * 8.0 / 1e9
    );

    if verify {
        match verify_copy(inf, ouf, infile_size) {
            Ok(msg) => eprintln!("{}", msg),
            Err(e) => {
                eprintln!("File copy verification error: {}", e);
                // Want to clean up file here but this might get run with sudo.
                eprintln!("Go clean up the invalid copy at {}", ouf);
                // Exit with a non-zero status code.
                std::process::exit(1);
            }
        }
    }
}
