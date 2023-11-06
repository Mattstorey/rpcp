# RPCP (Recursive Parallel Copy)

## Description
RPCP is a command-line tool designed for high-speed file copying, utilizing multiple threads to optimize bandwidth and transfer files quickly. It offers support for both individual files and recursive directory copying, with a focus on maximizing efficiency and throughput. This is still under development but works for the purpose of copying files and directories where bandwidth can be increased by making parallel calls to the source device. This is generally useful for retrieving data from NAS devices.  
The tool slices the input file(s) into segments and leverages multi-threading to expedite file transfers copying each slice simultaneously. The number of threads determines how many slices the file is divided into, and users can balance speed against system resource consumption. As threads copy their respective segments, RPCP ensures synchronized writing to the destination, preserving the file's integrity and order.  

## Features
- **Multi-threaded Copying:** Accelerate the copy process by running multiple threads in parallel.
- **Recursive Directory Copying:** Seamlessly copy entire directory structures.
- **Copy Verification:** Optional verification step to confirm the integrity of copied data.
- **Adjustable Thread Count:** Customize the number of threads used for copying.

## Installation
Ensure Rust and Cargo are installed on your system, then follow these steps:

1. Clone the RPCP repository:
- git clone https://github.com/Mattstorey/rpcp.git
- cd rpcp

2. Build the application with Cargo:
- cargo build --release

3. The compiled binary will be located in `target/release`.

## Usage
To copy files or directories with RPCP, use the following syntax:

- Copy a single file:
`rpcp source_file target_file`


- Copy directories recursively:
`rpcp -r source_directory target_directory`


- Adjust the number of threads (e.g., 32 threads):
`rpcp -t 32 source_file target_file`


- Verify the copy upon completion (for single file copy only):
`rpcp -v source_file target_file`


Run `rpcp --help` for more detailed information.

## Options
- `-t, --threads <THREADS>`: Set the number of threads to be used. [default: 10]
- `-r, --recursive`: Enable recursive copying for directories.
- `-v, --verify`: Verify the source and copied file are identical after copying.
- `-h, --help`: Show the help information.
- `-V, --version`: Display the version number of RPCP.

## Current Limitations
- **File Allocation (`fallocate`):** The `fallocate` optimization is currently under development and not yet functional.
- **Progress Bar:** The progress bar implementation is in progress and may not accurately reflect the current state of file copying.
- **Verify copy:** This only works for single file copy mode, for recursive copy of a directory each file would need to be checked and this would take to long, this tools is about speeding up copying. If the tool does not crash it can be reasonably expected the copying was successful. 
- **Disk space check:** RPCP does not check if you have enough disk-space to copy to the destination, again, this would slow it down. Use your best judgement for now, the tools will crash during the copy procedure if there is not enough space.  
