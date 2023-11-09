use clap::Parser;
use nix::sys::uio::{pread, pwrite};
use std::io;
use std::io::Read;
use std::path::Path;
use std::sync::{atomic::AtomicUsize, atomic::Ordering, Arc};
use std::thread;
use std::{
    fs::{create_dir_all, File},
    path::PathBuf,
};
use walkdir::WalkDir;

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
    ///Copy all file in source directory to destination directory
    recursive: bool,
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

fn copy_file<P: AsRef<Path>>(
    infile_path: P,
    outfile_path: P,
    num_threads: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut num_threads = num_threads;
    let infile = File::open(infile_path.as_ref()).map_err(|e| match e.kind() {
        io::ErrorKind::NotFound => {
            format!(
                "The input file {} does not exist. Please check the file path and try again.",
                infile_path.as_ref().display()
            )
        }
        _ => format!(
            "Failed to open input file: {}, {:?}",
            infile_path.as_ref().display(),
            e
        ),
    })?;
    let infile_size = infile.metadata()?.len() as usize;

    if infile_size < 1 * 1024 * 1024 {
        eprintln!("Samll file. Copy with one thread");
        num_threads = 1
    };
    let outfile = File::create(outfile_path.as_ref()).map_err(|e| {
        format!(
            "Failed to create output file '{}': {:?}",
            outfile_path.as_ref().display(),
            e
        )
    })?;
    outfile.set_len(infile_size as u64).unwrap();

    let mut threads = Vec::new();
    let slice = infile_size / num_threads;
    let processed_bytes = Arc::new(AtomicUsize::new(0));

    eprintln!(" Copy {}", infile_path.as_ref().display());

    //Wrap infiles in atomic reference counter.
    let infile = Arc::new(infile);
    let outfile = Arc::new(outfile);

    for thrd_num in 0..num_threads {
        let infile = Arc::clone(&infile);
        let outfile = Arc::clone(&outfile);
        let processed_bytes = Arc::clone(&processed_bytes);

        let t = thread::spawn(move || {
            let mut buffer = vec![0; 1024 * 1024];
            let mut pos = thrd_num * slice;

            while pos < (thrd_num + 1) * slice {
                let size_bytes_read = pread(&*infile, &mut buffer, pos as i64).unwrap();
                if size_bytes_read > 0 {
                    pwrite(&*outfile, &buffer[..size_bytes_read], pos as i64).unwrap();
                    pos += size_bytes_read;
                    processed_bytes.fetch_add(size_bytes_read, Ordering::SeqCst);
                } else {
                    break;
                }
            }
        });
        threads.push(t);
    }

    // Progress monitoring thread
    let progress_clone = Arc::clone(&processed_bytes);

    let monitor_handle = thread::spawn(move || {
        while progress_clone.load(Ordering::SeqCst) < infile_size {
            let pct_prgrs =
                (progress_clone.load(Ordering::SeqCst) as f64 / infile_size as f64) * 100.;
            eprint!("\rProgress: {pct_prgrs:.1}%",);
            thread::sleep(std::time::Duration::from_millis(50)); // Update every .25 second
        }
        eprint!("\rProgress: 100.0%",);
    });

    for t in threads {
        t.join().unwrap();
    }

    monitor_handle.join().unwrap();
    Ok(infile_size)
}

fn copy_dir_recursive(
    src: &Path,
    dest: &Path,
    num_threads: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut total_bytes_copied = 0;
    for entry in WalkDir::new(src) {
        let entry = entry?;
        let path = entry.path();
        let relative_path = path.strip_prefix(src)?;
        let dest_path = dest.join(relative_path);
        eprint!("\r");
        if path.is_dir() {
            create_dir_all(&dest_path)?;
        } else {
            let bytes_copied = copy_file(path, &dest_path, num_threads)?;
            total_bytes_copied += bytes_copied;
        }
    }
    Ok(total_bytes_copied)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let inf = cli.in_file;
    let ouf = cli.out_file;
    let num_threads = cli.threads as usize;

    eprintln!("Copying data with {} threads", num_threads);

    // do recursive dir walk here
    let start_time = time_as_double().map_err(|e| format!("Error calculating time: {:?}", e))?;

    let (copy_size, finish_time) = (|| -> Result<(usize, f64), Box<dyn std::error::Error>> {
        if !cli.recursive {
            let copy_size = copy_file(&inf, &ouf, num_threads)?;
            let finish_time =
                time_as_double().map_err(|e| format!("Error calculating time: {:?}", e))?;
            Ok((copy_size, finish_time))
        } else {
            let copy_size = copy_dir_recursive(&inf, &ouf, num_threads)?;
            let finish_time =
                time_as_double().map_err(|e| format!("Error calculating time: {:?}", e))?;
            Ok((copy_size, finish_time))
        }
    })()?;

    eprintln!(
        "\n Copy finished. {} bytes written in {:.1} seconds = {:.3} Gbits/s",
        copy_size,
        finish_time - start_time,
        copy_size as f64 / (finish_time - start_time) * 8.0 / 1e9
    );

    // varify only works for single file copy mode for now
    if !cli.recursive & cli.verify {
        match verify_copy(&inf, &ouf, copy_size) {
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
