
# USED TO CLEAR THE ERROR DURING TRANSFORMATION-JSON FILE AS IF CORRUPTED WITH UNKNOWN VALUES

import json
def clean_json_file(input_file, output_file):
  
   with open(input_file, 'r', encoding='utf-8') as file:
       content = file.read()


   # To remove any non-printable or hidden characters
   cleaned_content = ''.join(char for char in content if char.isprintable())



   with open(output_file, 'w', encoding='utf-8') as file:
       file.write(cleaned_content)


   return cleaned_content

input_file_path = '/content/hist_edited.json'
output_file_path = '/content/hist_edited.json'


# Clean the JSON file content and save it as a new file
cleaned_content = clean_json_file(input_file_path, output_file_path)
print("Cleaned content saved to:", output_file_path)


