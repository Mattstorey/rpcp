use nix::sys::uio::{pread, pwrite};
use std::env;
use std::fs::File;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::thread;

fn time_as_double() -> f64 {
    let now = std::time::SystemTime::now();
    let since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    since_epoch.as_secs_f64()
}

fn verify_files(file1: &str, file2: &str, fs: usize) {
    eprintln!("*info* verifying '{}' and '{}', size {}", file1, file2, fs);
    let mut in1 = File::open(file1).unwrap();
    let mut in2 = File::open(file2).unwrap();
    let mut buffer1 = vec![0; 10 * 1024 * 1024];
    let mut buffer2 = vec![0; 10 * 1024 * 1024];

    for i in (0..fs).step_by(10 * 1024 * 1024) {
        let ret1 = in1.read(&mut buffer1).unwrap();
        let ret2 = in2.read(&mut buffer2).unwrap();
        if ret1 == ret2 {
            if &buffer1[..ret1] != &buffer2[..ret2] {
                eprintln!("*error* file different at range starting {}", i);
                std::process::exit(3);
            }
        } else {
            eprintln!("*warning* uneven reads");
        }
    }
    eprintln!("*info* files are identical");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage: {} <infile> <outfile> [num_threads]", args[0]);
        std::process::exit(1);
    }

    let inf = &args[1];
    let ouf = &args[2];
    let num_threads = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(12);
    let verify = false; // todo add arg parsing
    let infile = File::open(inf).unwrap();
    let infile_size = infile.metadata().unwrap().len() as usize;
    eprintln!(
        "*info* infile size: {}, threads {}",
        infile_size, num_threads
    );
    let infile = Arc::new(infile);
    let outfile = File::create(ouf).unwrap();
    outfile.set_len(infile_size as u64).unwrap();
    let outfile = Arc::new(outfile);

    let mut threads = Vec::new();
    let slice = infile_size / num_threads;
    let start_time = time_as_double();

    let processed_bytes = Arc::new(Mutex::new(0usize));

    for i in 0..num_threads {
        let infile = Arc::clone(&infile);
        let outfile = Arc::clone(&outfile);
        let processed_bytes = Arc::clone(&processed_bytes);

        let t = thread::spawn(move || {
            let mut buffer = vec![0; 1024 * 1024];
            let mut pos = i * slice;
            let mut local_processed = 0usize;

            while pos < (i + 1) * slice {
                let sz = pread(&*infile, &mut buffer, pos as i64).unwrap();
                if sz > 0 {
                    pwrite(&*outfile, &buffer[..sz], pos as i64).unwrap();
                    pos += sz;
                    local_processed += sz;
                } else {
                    break;
                }
                if local_processed >= 10 * 1024 * 1024 {
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
        verify_files(inf, ouf, infile_size);
    }
}
