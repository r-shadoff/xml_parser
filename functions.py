from ftplib import FTP
import os
import tarfile
import shutil
from bs4 import BeautifulSoup
import re
import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher, DependencyMatcher
from spacypdfreader.spacypdfreader import pdf_reader

def download_pmc(dir, subdir, download_path):
    """
    :params dir and subdir:
    The PMC open access archive is organized by two levels of directories, with each directory 
    containing multiple publications. To specify the folder you would like to download, assign 
    dir and subdir as follows: 
    https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/<dir>/<subdir>
    Example: download_pmc("00", "01", download_path) retrieves the directory found at: 
    https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/00/01
   
    :param download_path:
    PMC directories will be downloaded and saved to the specified location for downstream processing.
    """
    server = "ftp.ncbi.nlm.nih.gov"
    server_directory = "/pub/pmc/oa_package/"
    full_directory_path = os.path.join(server_directory, dir, subdir)

    # Create a local folder to store the downloads
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Connect to FTP server
    ftp = FTP(server)
    ftp.login()

    # Change to the target directory
    ftp.cwd(full_directory_path)

    # List files in the directory
    files = ftp.nlst()  # Get list of all files in the directory

    print(f"Found {len(files)} files. Starting download...")

    # Download each file
    for file in files:
        downloaded_file_path = os.path.join(download_path, file)
        
        # Download the file in binary mode
        with open(downloaded_file_path, "wb") as f:
            ftp.retrbinary(f"RETR {file}", f.write)
        
        print(f"Downloaded: {file}")

    # Close FTP connection
    ftp.quit()

    print(f"Directory {dir}/{subdir} downloaded successfully!")

def uncompress_tar(download_path):
    """
    Uncompresses all tar archives in the provided folder and removes empty archives.
    :param download_path: path to downloaded PMC directory
    """
    tar_list = os.listdir(download_path)
    uncompressed_filepath = os.path.join(download_path, "Uncompressed")

    os.makedirs(uncompressed_filepath, exist_ok=True)
    for item in tar_list: # item = compressed archive
        compressed_filepath = os.path.join(download_path, item)
        if item.endswith(".tar.gz"):
            if os.path.getsize(compressed_filepath) > 0:
                if os.path.isfile(compressed_filepath): 
                    with tarfile.open(compressed_filepath, "r") as tar:
                        try:
                            tar.extractall(path = uncompressed_filepath)
                            print(f'{item} has been uncompressed succesfully.')
                            os.remove(compressed_filepath)
                            print(f'{item} archive has been removed.')
                        except EOFError:
                            print("Corrupted tar archive. Moving to next file.")
                else:
                    pass # skips directories 
            else:
                if os.path.isfile(compressed_filepath):
                    os.remove(compressed_filepath) # Removes empty files
                elif os.path.isdir(compressed_filepath):
                    os.rmdir(compressed_filepath) # Removes empty directories
        else:
            pass

def sort_data(download_path):
    """
    Checks if each subdir is viable for figure and caption analysis by searching for image files that are referenced in the nxml.
    :param download_path: filepath for the parent folder that holds the PMC folders to process.
    """

    downstream_processing = []
    to_remove = [] 

    for root, dirs, files in os.walk(download_path):
        parent_folder = os.path.basename(os.path.dirname(root))  
        for subdir in dirs:  # each subdir is one PMC record
            subdir_path = os.path.join(root, subdir)
            for file_info in os.walk(subdir_path):  # Looks at all files in the PMC record dir
                pmc_record_path = file_info[0]
                pmc_record_contents = file_info[2]  # List of files in the folder
                
                # Extract PMC ID
                record_id_match = re.match(r".*/(PMC\d+)$", pmc_record_path)
                record_id = record_id_match.group(1) if record_id_match else None
                if not record_id:
                    continue  # Skip if record ID is not found
                
                # Check for images
                images = (".png", ".jpg", ".gif")
                if any(file.endswith(images) for file in pmc_record_contents):
                    print(f"Image found in record {record_id}")
                    
                    # Find nxml file
                    nxml_file = next((file for file in pmc_record_contents if file.endswith(".nxml")), None)
                    if nxml_file:
                        print(f"XML file found in record {record_id}")
                        nxml_path = os.path.join(pmc_record_path, nxml_file)
                        
                        # Read XML content
                        with open(nxml_path, "r", encoding="utf-8") as xml_file:
                            content = xml_file.read()
                            soup = BeautifulSoup(content, features="xml")
                            
                            # Check for figure data
                            figures_present = soup.find_all("fig")
                            if figures_present:
                                print(f"Figure data found in XML contents for record {record_id}")
                                downstream_processing.append(record_id)
                            else:
                                print(f"No figure data in {record_id}")
                                to_remove.append(record_id)
                    else:
                        print(f"No XML associated with {record_id}")
                        to_remove.append(record_id)
                else:
                    print(f"No images associated with {record_id}")
                    to_remove.append(record_id)

    # Sort files into a folder for downstream analysis 
    output_filepath = os.path.join(download_path, "Sorted")
    os.makedirs(output_filepath, exist_ok=True)  # Ensure destination folder exists
    
    for item in downstream_processing:
        source = os.path.join(download_path, "Uncompressed")
        source2 = os.path.join(source, item)
        destination = os.path.join(output_filepath, item)
        if os.path.exists(source2):
            shutil.move(source2, destination)
        else:
            print(f"Warning: Source folder {source} not found, skipping move.")


    for item in to_remove:
        source3 = os.path.join(download_path, "Uncompressed")
        source4 = os.path.join(source3, item)
        
        if os.path.exists(source4):
            shutil.rmtree(source4)
            print(f"{item} does not include sufficient data for processing and has been removed.")
        else:
            print(f"Warning: Directory {source4} not found, skipping removal.")

def grab_figure_data(download_path):
    """
    Extracts figure captions from all PMC records in the provided folder.
    :param download_path: filepath for the parent folder that holds the PMC folders to process.
    :return: a .tsv file with figure captions for each PMC record.
    """

    input_filepath = os.path.join(download_path, "Sorted")
    output_filepath = download_path
    output_file = os.path.join(download_path, "figure_data.tsv")
   
    all_figures = []
    for root, dirs, files in os.walk(input_filepath):  
        # print(f"Processing directory: {root}")  # Print the directory being processed

        for subdir in dirs:  # each subdir is one PMC record
            # print(f"Processing subdirectory: {subdir}")
            subdir_path = os.path.join(root, subdir)

            for file in os.walk(subdir_path): # Every directory is 1 PMC record
                # Initialize lists
                
                pmc_record_path = file[0]
                file_list = file[2] 
                image_list = []
                figure_ids = []
                associated_images = []
                figure_labels = []
                sentences_before = []
                sentences_after = []
                direct_ref_text = []
                caption_titles = []
                caption_texts = []
                raw_image_names = []

                # Extract record ID
                record_id = re.match(r".*/(PMC\d+)$", pmc_record_path)
                if record_id:
                    record_id = record_id.group(1)
                

                for item in file_list:
                    if item.endswith(".nxml"): # Process XML file
                        # Read the content of the XML file
                        xml_filepath = os.path.join(file[0], item)
                        print(f"Processing file: {xml_filepath}")
                        
                        with open(xml_filepath, "r", encoding="utf-8") as xml_file:
                            content = xml_file.read()
                            soup = BeautifulSoup(content, features="xml")

                            # Global search for all figure tags
                            figures = soup.find_all(re.compile(r"^[Ff]ig$"))
                            for figure in figures:
                                # Extract figure data
                                figure_id = figure.get("id").strip().replace("\n", " ")
                                
                                label_tag = figure.find("label")
                                if label_tag:
                                    figure_label = label_tag.text.strip().replace("\n", " ")
                                else:
                                    figure_label = "No figure label"

                                image_ref_tag = figure.find("graphic")
                                if image_ref_tag:
                                    associated_image = image_ref_tag.get("xlink:href")
                                else:
                                    associated_image = "Not found"
                                
                                xrefs_in_doc = soup.find_all("xref") # Returns list of all xrefs
                                for ref in xrefs_in_doc:
                                    if ref:
                                        ref_id = ref.get('rid')
                                        if ref_id:
                                            ref_id = ref_id.strip().replace("\n", " ")
                                            if ref_id == figure_id :
                                                before_text = ref.find_parent('p')
                                                if before_text:
                                                    after_text = before_text.find_next_sibling('p')
                                                else:
                                                    sentences_before = "No text before"

                                                if before_text and after_text:
                                                    sentences_before = before_text.get_text().strip().replace("\n", " ")
                                                    sentences_after = after_text.get_text().strip().replace("\n", " ")
                                                    break
                                                elif before_text:
                                                    sentences_before = before_text.get_text().strip().replace("\n", " ")
                                                    sentences_after = "No text after"
                                                    break
                                                else:
                                                    sentences_after
                                            else:
                                                sentences_before = "No text before"
                                        else:
                                            sentences_before = "No xref found"
                                            sentences_after = "No xref found"
                                else:
                                    sentences_before = "No xref found"
                                    sentences_after = "No xref found"
                                
                                xrefs_in_doc = soup.find_all("xref") # Returns list of all xrefs
                                for ref in xrefs_in_doc:
                                    if ref:
                                        ref_id = ref.get('rid')
                                        if ref_id:
                                            ref_id = ref_id.strip().replace("\n", " ")
                                            if ref_id == figure_id :
                                                parent = ref.find_parent('p')
                                        else:
                                            ref_id = "No xref found"        
                                    else:
                                        ref_id = "No xref found"

                                caption_title_text = "No caption title"
                                caption_text_text = "No caption text"
                                # Extract caption data 
                                captions = figure.find_all(re.compile(r"^[Cc]aption$"))
                                for caption in captions:
                                    caption_title = caption.find("title")
                                    caption_title_text = caption_title.text.strip().replace("\n", " ") if caption_title else "No caption title"
                                    caption_text = caption.find("p")
                                    caption_text_text = caption_text.text.strip().replace("\n", " ") if caption_text else "No caption text"
                                
                                # Export to dictionary (one dict per figure)
                                all_figures.append({
                                    "PMC ID": record_id,
                                    "Figure ID": figure_id,
                                    "Figure Label": figure_label,
                                    "Associated Image File": associated_image,
                                    "Sentences Before": sentences_before,
                                    "Sentences After": sentences_after,
                                    "Caption Title": caption_title_text,
                                    "Caption Text": caption_text_text
                                })

    df = pd.DataFrame(all_figures, columns=[
            "PMC ID", "Figure ID", "Figure Label", "Associated Image File", 
            "Sentences Before", "Sentences After", "Caption Title", "Caption Text"
        ])

    # Export data
    
    output_csv = os.path.join(output_filepath, output_file)
    os.makedirs(output_filepath, exist_ok=True)

    df.to_csv(output_csv, sep='\t', index=False, encoding="utf-8")
    print(f"Extraction complete. CSV saved to {output_csv}")

def grab_spacy_text(download_path):
    input_filepath = os.path.join(download_path, "Sorted")
    output_filepath = download_path
    output_file = os.path.join(download_path, "spacy_figure_data.tsv")
    
    sentence_data = []
    nlp = spacy.load("en_core_web_sm")
    terms = ["Figure", "Fig", "figure", "fig", "fig. ", "Fig. "]
    matcher = PhraseMatcher(nlp.vocab)


    for root, dirs, files in os.walk(input_filepath):  
        # print(f"Processing directory: {root}")  # Print the directory being processed

        for subdir in dirs:  # each subdir is one PMC record
            print(f"Processing subdirectory: {subdir}")
            subdir_path = os.path.join(root, subdir)
            for dirpath, _, filenames in os.walk(subdir_path):
                for filename in filenames:
                    item_path = os.path.join(dirpath, filename)
                    if item_path.endswith(".nxml"):
                        with open(item_path, "r") as file: 
                            
                            soup = BeautifulSoup(file, "xml")
                            for xref in soup.find_all("xref", {"ref-type": "bibr"}): # Remove inline citations 
                                xref.decompose()
                            for id in soup.find_all("object-id", {"pub-id-type" : "doi"}):
                                id.decompose()
                            text = soup.get_text(separator = ' ')
                            text = re.sub(r'\s+', ' ', text).strip()
                            
                            
                            doc = nlp(text)
                            sentences = list(doc.sents)

                            for i, sentence in enumerate(sentences):
                                if any(term in sentence.text for term in terms):      
                                    # Include next sentence if sentence ends in "Fig."
                                    if sentence.text.strip().endswith("Fig."):
                                        next_sentence = sentences[i + 1].text if i + 1 < len(sentences) else ""
                                        sentence_data.append([subdir, sentence.text + " " + next_sentence])
                                        
                                    else:
                                        sentence_data.append([subdir, sentence.text])
                                        
                                                    
    df = pd.DataFrame(sentence_data, columns=["PMC ID", "Sentences"])
    df.to_csv(output_file, sep="\t")
    print(f"Spacy text extracted: {output_file}")

def combine_dataframes(download_path):
    df1_path = os.path.join(download_path, "figure_data.tsv")
    df2_path = os.path.join(download_path, "spacy_figure_data.tsv")
    df_combined_path = os.path.join(download_path, "combined_figure_data.tsv")
    df1 = pd.read_csv(df1_path, sep="\t")
    df2 = pd.read_csv(df2_path, sep="\t")




    merged_rows = []

    # Loop over df1 rows
    for id1, row1 in df1.iterrows():
        fig_id = str(row1['Figure ID'])
        fig_label = str(row1['Figure Label'])

        # Filter df2 for rows where either fig_id or fig_label is a substring 
        matches = df2[df2['Sentences'].str.contains(fig_id, na=False) | df2['Sentences'].str.contains(fig_label, na=False)]

        if not matches.empty:
            for _, row2 in matches.iterrows():
                # Append the merged row to the list
                merged_row = row1.to_dict()
                merged_row['Spacy Extracted Text'] = row2['Sentences']
                merged_rows.append(merged_row)
        else:
            # Optional: include df1 rows with no match
            merged_row = row1.to_dict()
            merged_row['Spacy Extracted Text'] = None
            merged_rows.append(merged_row)

    # Convert the result to a new DataFrame
    df_combined = pd.DataFrame(merged_rows)
    df_combined.to_csv(df_combined_path, sep="\t")
    print(f"Dataframes combined: {df_combined_path}")

def clean_text(download_path):
    """
    Removes whitespace, line breaks, and invisible characters from text strings. 
    :param download_path: the figure_data.tsv within this directory will be cleaned.
    :return: a cleaned .tsv file
    """
    def simple_clean_text(text):
        text = str(text).replace('\xa0', ' ') 
        text = re.sub(r'[^\S\t]+', ' ', text)
        return text.strip()

    def clean_latex_text(text):
        text = re.sub(r'\\(?:documentclass\[[^\]]*\]\{[^\}]*\}|usepackage\{[^\}]*\}|setlength\{[^\}]*\}|begin\{[^\}]*\}|end\{[^\}]*\}|[a-zA-Z]+\{[^\}]*\})', '', text)
        text = re.sub(r'(\$[^\$]*\$|\\\([^\)]*\\\))', lambda m: m.group(0), text)
        return text.strip()
    
    def clean_spacy_text(text):
        text = re.sub(r'\((?:[A-Za-z\s\.\-]+(?:,|\set\sal\.,?|\sand\s[A-Za-z\s\.\-]+,?)\s?\d{4}(?:;?\s?)?)+\)', '', text)
        return text.strip()

    df_path = os.path.join(download_path, "combined_figure_data.tsv")
    output_path = os.path.join(download_path, "combined_cleaned_data.tsv")
    df = pd.read_csv(df_path, sep="\t")
    df['Spacy Extracted Text'] = df['Spacy Extracted Text'].fillna('').astype(str)
    merged_df = df.groupby(['PMC ID', 'Figure ID', 'Figure Label', 'Associated Image File', 'Sentences Before', 'Sentences After', 'Caption Title', 'Caption Text'], as_index=False).agg({'Spacy Extracted Text': ' '.join})
    text_cols = ('Sentences Before', 'Sentences After', 'Caption Title', 'Caption Text', 'Spacy Extracted Text')
    for col in text_cols:
        merged_df[col] = merged_df[col].map(simple_clean_text)
        merged_df[col] = merged_df[col].map(clean_latex_text)
        merged_df[col] = merged_df[col].map(clean_spacy_text)


    print(f"{df_path} has been cleaned.")

    merged_df.to_csv(output_path, sep="\t")
    print(f"Cleaned csv saved to {output_path}")

def file_shuttle(download_path):
    """
    Moves files from the Sorted folder back to the parent folder, so input files and output TSVs are within the same directory.
    """
    source_path = os.path.join(download_path, "Sorted")
    destination_path = download_path

    for dir in os.listdir(source_path):
        full_in_path = os.path.join(source_path, dir)
        full_out_path = os.path.join(destination_path, dir)
        shutil.move(full_in_path, full_out_path)
        print(f"Folder {dir} moved to {full_out_path}")

def remove_file_type(download_path, extensions):
    """
    Remove files from subdirectories if they end with the specified extension. CASE SENSITIVE.
    :param input_folderpath: string. Input the folder with subdirectories to remove files from
    :param extensions: string. the file type to remove. can accept single arguments or a list of arguments.
    """

    all_contents = os.listdir(download_path)
    for item in all_contents:
        item_path = os.path.join(download_path, item)
        if os.path.isdir(item_path):
            files = os.listdir(item_path)
            for file in files: 
                print(f"Processing {file}")
                file_path = os.path.join(item_path, file)
                ext = os.path.splitext(file_path)
                ext_txt = str(ext[1])
              
                for extension in extensions:
                    if ext_txt == extension:
                        os.remove(file_path)
                        print(f"Deleted {file_path}")
                    else:
                        pass
        else:
            pass
        
def unique_exts(download_path):
    """
    Provides a list of unique extensions in a folder. Good for troubleshooting/checking results. 
    :param download_path: string. Input the folder with subdirectories to remove gifs from
    """

    all_contents = os.listdir(download_path)
    for item in all_contents:
        unique_exts= []
        item_path = os.path.join(download_path, item)
        if os.path.isdir(item_path):

            files = os.listdir(item_path)
            for file in files: 
                
                file_path = os.path.join(item_path, file)
                ext = os.path.splitext(file_path)
                ext_txt = str(ext[1])
                if ext_txt not in unique_exts:
                    unique_exts.append(ext_txt)
                
        else:
            pass
        
        print(f"The unique extensions in folder {item} are {unique_exts}") 
        if ".pdf" not in unique_exts:
            print(f"WARNING: {item} DOES NOT CONTAIN PDF FILE")      

def no_trace(download_path):
    """
    Removes intermediate directories created during processing.
    :param parent_folder: parent folder of PMC record folders.
    """
    uncompressed_folder = os.path.join(download_path, "Uncompressed")
    sorted_folder = os.path.join(download_path, "Sorted")
    try:
        shutil.rmtree(uncompressed_folder)
        shutil.rmtree(sorted_folder)
    except FileNotFoundError:
        print("Folder to remove not found.")
    print("Poof! All intermediate files generated by this pipeline have been erased.")