from deep_translator import GoogleTranslator
import ruamel.yaml
import time
import random
import re
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Set-up
input_file = 'it.yml'
output_file = 'en.yml'
source_lang = 'it'
target_lang = 'en'
workers = 3
retry_limit = 1
pause_time = 60  #seconds before 2nd try
request_interval = 1  #seconds between requests

# Inizializza traduttore
translator = GoogleTranslator(source=source_lang, target=target_lang)

PLACEHOLDERS = [
    r"%\w+%",
    r"&\w+",
    r"'[A-Za-z]+'",
]

temp_words = ['SOMETHING', 'SOMEONE', 'PLACEHOLDER']

def translate_text(key, text):
    if not text:
        print(f"No text to translate for key: {key}")
        return text

    print(f"Traslating: {key} | {text}")  #per debug
    placeholders = []

    # Replace placeholders with temporary tokens
    for ph in PLACEHOLDERS:
        matches = re.findall(ph, text)
        for i, match in enumerate(matches):
            placeholder_id = f"PLACEHOLDER{i}"
            placeholders.append((placeholder_id, match))
            text = text.replace(match, placeholder_id)

    # prevent Rate limit error with time sleep
    time.sleep(request_interval)

    # Manage retries
    for attempt in range(retry_limit + 1):
        try:
            translated_text = translator.translate(text)
            break
        except Exception as e:
            print(f"Attempt {attempt + 1} - Error translating key: {key}. Error: {e}")
            if "429" in str(e) or "Server Error" in str(e):  # Rate limit error
                print("Rate limit error encountered. Stopping execution immediately.")
                sys.exit("Stopping execution due to rate limit errors.") #Stop execution with Rate limit error
            else:
                print(f"Permanent error translating key: {key}. Error: {e}")
                return text
    else:
        print(f"Failed to translate key: {key} dopo {retry_limit} tentativi.")
        return text

    # Restore original placeholders
    for i in range(len(placeholders)):
        translated_text = translated_text.replace(temp_words[i % len(temp_words)], placeholders[i][0])

    for placeholder_id, original in placeholders:
        pattern = re.compile(placeholder_id, re.IGNORECASE)
        translated_text = pattern.sub(original, translated_text)

    return translated_text.replace('\n', ' ')

def flatten_yaml(data, parent_key=''):
    """Flatten nested YAML dictionary."""
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}.{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_yaml(v, new_key))
        elif isinstance(v, str):
            items.append((new_key, v))
    return items

def unflatten_yaml(flattened_data):
    """Unflatten dictionary back to nested structure."""
    nested_dict = {}
    for key, value in flattened_data:
        keys = key.split('.')
        d = nested_dict
        for k in keys[:-1]:
            d = d.setdefault(k, {})
        d[keys[-1]] = value
    return nested_dict

def save_progress(processed_keys, progress_file='progress.json'):
    """Save progress to a file."""
    with open(progress_file, 'w') as f:
        json.dump(processed_keys, f)

def load_progress(progress_file='progress.json'):
    """Load progress from a file."""
    try:
        with open(progress_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def translate_yaml(input_file, output_file, source_lang, target_lang, workers):
    yaml = ruamel.yaml.YAML()
    yaml.preserve_quotes = True

    # Load YAML content
    with open(input_file, 'r', encoding='utf-8') as stream:
        try:
            yaml_content = yaml.load(stream)
            print(f"Loading content YAML from {input_file}")
        except Exception as exc:
            print(f"Error uploading YAML: {exc}")
            return

    flattened_content = flatten_yaml(yaml_content)
    processed_keys = load_progress()

    # Decrease workers to prevent exceeding rate limits
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {key: executor.submit(translate_text, key, value) for key, value in flattened_content if key not in processed_keys}

        translated_flattened_content = []
        for future in as_completed(futures.values()):
            key = next(k for k, v in futures.items() if v == future)  # Find the key for the completed future
            try:
                result = future.result()
                if result:
                    translated_flattened_content.append((key, result))
                    processed_keys.append(key)
                    save_progress(processed_keys)
                else:
                    print(f"Received empty result for key: {key}")
            except Exception as e:
                print(f"Error in future result for key {key}: {e}")

    # Unflatten content and update YAML
    translated_content = unflatten_yaml(translated_flattened_content)
    yaml_content.update(translated_content)

    # Finalize output file
    with open(output_file, 'w', encoding='utf-8') as outfile:
        yaml.dump(yaml_content, outfile)
        print(f"Output file: {output_file}")

if __name__ == "__main__":
    print("Starting Traslation...")
    translate_yaml(input_file, output_file, source_lang, target_lang, workers)
    print("Translation Finished.")
