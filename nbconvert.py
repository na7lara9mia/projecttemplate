import os import json import nbformat from nbformat.v4 import new_notebook, new_code_cell

def convert_json_to_ipynb(src_folder, dest_root_folder): for root, _, files in os.walk(src_folder): for file in files: if file.endswith(".json"): json_path = os.path.join(root, file)

# Build the new directory structure
            relative_path = os.path.relpath(root, src_folder)
            new_folder_path = os.path.join(dest_root_folder, relative_path)
            os.makedirs(new_folder_path, exist_ok=True)
            
            # Load JSON and extract code cells
            with open(json_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                    
                    # Create a new notebook
                    nb = new_notebook()
                    
                    # Assuming code cells are stored under a 'cells' key
                    for cell in data.get("cells", []):
                        if cell.get("cell_type") == "code":
                            code_content = "\n".join(cell.get("source", []))
                            nb.cells.append(new_code_cell(code_content))
                    
                    # Save the notebook
                    ipynb_filename = os.path.splitext(file)[0] + ".ipynb"
                    ipynb_path = os.path.join(new_folder_path, ipynb_filename)
                    
                    with open(ipynb_path, "w", encoding="utf-8") as nb_file:
                        nbformat.write(nb, nb_file)
                        
                    print(f"Converted: {json_path} -> {ipynb_path}")
                
                except json.JSONDecodeError:
                    print(f"Skipping {json_path}: Invalid JSON format")

Example usage

source_folder = "source_json_files" destination_folder = "converted_ipynb_files" convert_json_to_ipynb(source_folder, destination_folder)

Let me know if you'd like me to add more features or refine anything! I can make the code handle more edge cases or add logging if needed.

