#!/bin/bash

# Path to the folder containing the files
SOURCE_FOLDER="/Users/socoobo/Downloads/pt-crawl"

# Base name for the destination folders
DEST_BASE="/Users/socoobo/Projects/Sharders/worker"

# Number of destination folders
NUM_FOLDERS=10

# Create the destination folders if they don't exist
for i in $(seq $NUM_FOLDERS); do
  rm -r "$DEST_BASE/$i/pt-crawl"
  mkdir -p "$DEST_BASE/$i/pt-crawl"
done

# Count the total number of files in the source folder
TOTAL_FILES=$(find "$SOURCE_FOLDER" -type f | wc -l)

# Calculate the chunk size
CHUNK_SIZE=$(( (TOTAL_FILES + NUM_FOLDERS - 1) / NUM_FOLDERS ))

# Distribute files in chunks
folder_index=1
file_count=0

for file in "$SOURCE_FOLDER"/*; do
  # Move the file to the current folder
  cp "$file" "$DEST_BASE/$folder_index/pt-crawl/"

  # Increment the file count
  file_count=$((file_count + 1))

  # If the current folder has enough files, move to the next folder
  if [ $file_count -ge $CHUNK_SIZE ] && [ $folder_index -lt $NUM_FOLDERS ]; then
    folder_index=$((folder_index + 1))
    file_count=0
  fi
done

echo "Files have been chunked across $NUM_FOLDERS folders."
