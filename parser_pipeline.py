# Import Necessary Libraries
import os
import functions as f

download_path = "/Users/rachel/Desktop/xml_parser_test"

# Step 1. Download folder contents
f.download_pmc("00", "00", download_path)

# Step 2. Uncompress archives 
f.uncompress_tar(download_path)

# Step 3. Sort viable records
f.sort_data(download_path)

# # Step 4. Parse Sorted Records
f.grab_figure_data(download_path)
f.clean_text(download_path)

# # Step 5. Clean up
f.file_shuttle(download_path)
f.remove_file_type(download_path, extensions=(".gif", ".doc", ".docx", ".html", ".mov"))
f.unique_exts(download_path) 
f.no_trace(download_path)
