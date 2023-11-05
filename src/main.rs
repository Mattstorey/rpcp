use clap::Parser;
use nix::sys::uio::{pread, pwrite};
use std::io;
use std::io::Read;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use std::thread;
use std::{fs::File, path::PathBuf};

#[derive(Parser)]
#[command(name = "Parallel copy")]
#[command(author = "Matt S. <matt.storey@netvalue.nz>")]
#[command(version = "0.1.0")]
#[command(about = "Threaded copying of files to steal bandwidth", long_about = None)]
struct Cli {
    ///Source file path
    in_file: PathBuf,
    ///Destination file path
    out_file: PathBuf,
    #[arg(short, long, default_value_t = 10)]
    threads: u8,
    #[arg(short, long)]
    /// Verifies the copy completed successfully
    verify: bool,
}

fn time_as_double() -> Result<f64, std::time::SystemTimeError> {
    // High precision time.
    let now = std::time::SystemTime::now();
    let since_epoch = now.duration_since(std::time::UNIX_EPOCH)?;
    Ok(since_epoch.as_secs_f64())
}

fn verify_copy(
    file1: &PathBuf,
    file2: &PathBuf,
    file_size: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    eprintln!(
        "Verifying '{}' and '{}' are the same after copy. Size {}",
        file1.display(),
        file2.display(),
        file_size
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
                return Err(format!("File differ at range starting at {} bytes", step).into());
            }
        } else {
            eprintln!("*warning* uneven reads during varificaion");
        }
    }
    Ok("Verified files are identical.".into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let inf = cli.in_file;
    let ouf = cli.out_file;
    let num_threads = cli.threads as usize;

    let infile = File::open(&inf).map_err(|e| match e.kind() {
        io::ErrorKind::NotFound => {
            "The input file does not exist. Please check the file path and try again.".into()
        }
        _ => format!("Failed to open input file: {}, {:?}", &inf.display(), e),
    })?;

    let infile_size = infile
        .metadata()
        .map_err(|e| format!("Failed to get file size metadata: {:?}", e))?
        .len() as usize;

    eprintln!(
        "Copying. Infile size: {}, with threads {}",
        infile_size, num_threads
    );

    //Wrap infile in atomic reference counter.
    let infile = Arc::new(infile);

    //Set up output file
    let outfile = File::create(&ouf).map_err(|e| {
        format!(
            "Failed to produce output file '{}': {:?}",
            &ouf.display(),
            e
        )
    })?;

    outfile.set_len(infile_size as u64).unwrap();

    let outfile = Arc::new(outfile);

    let mut threads = Vec::new();
    let slice = infile_size / num_threads;
    let start_time = time_as_double().map_err(|e| format!("Error calculating time: {:?}", e))?;

    let one_tenth_file_size = infile_size / 10;

    let processed_bytes = Arc::new(AtomicUsize::new(0));

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
                // TODO: Dodgy progress bar -- needs work...
                if local_processed >= one_tenth_file_size {
                    processed_bytes.fetch_add(local_processed, Ordering::Relaxed);
                    eprint!(
                        "\r[progress] {:.1}%",
                        (processed_bytes.load(Ordering::Relaxed) as f64 / infile_size as f64)
                            * 100.0
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
    let finish_time = time_as_double().map_err(|e| format!("Error calculating time: {:?}", e))?;
    eprintln!(
        "\n Copy finished. {} bytes written in {:.1} seconds = {:.3} Gbits/s",
        infile_size,
        finish_time - start_time,
        infile_size as f64 / (finish_time - start_time) * 8.0 / 1e9
    );
    if cli.verify {
        match verify_copy(&inf, &ouf, infile_size) {
            Ok(msg) => eprintln!("{}", msg),
            Err(e) => {
                eprintln!("File copy verification error: {}", e);
                // Want to clean up file here but this might get run with sudo.
                eprintln!("Go clean up the invalid copy at {}", ouf.display());
                // Exit with a non-zero status code.
                std::process::exit(1);
            }
        }
    }
    Ok(())
}
