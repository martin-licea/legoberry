import polars as pl
from icecream import ic
import yaml
import re
from utils import clean_up_drop_fields, create_output_file
import logging 
from pathlib import Path
import sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
from pathlib import Path
ic.configureOutput(contextAbsPath=True)

# Cross-platform encoding safety for interactive features
def safe_print(text):
    """Print with cross-platform encoding safety"""
    try:
        print(text)
    except UnicodeEncodeError:
        # Fallback to ASCII-safe version for older systems
        safe_text = text.encode('ascii', 'replace').decode('ascii')
        safe_text = safe_text.replace('🔍', '*').replace('📍', '*').replace('📊', '*').replace('🔄', '*').replace('🔸', '>').replace('🔹', '>').replace('✅', '*')
        print(safe_text)

# Interactive Address Deduplication Classes
class InteractiveAddressProcessor:
    def __init__(self):
        self.user_decisions = {}  # Cache decisions for similar cases
        self.apply_to_all = False
        self.global_choice = None
    
    def display_comparison(self, original, extracted, city, state, zip_code, confidence, redundancy, field_values=None, duplicates_found=None):
        """Display a clean comparison of the address options"""
        safe_print("\n" + "="*80)
        safe_print("ADDRESS DEDUPLICATION REVIEW")
        safe_print("="*80)
        safe_print(f"📍 Context: {city}, {state} {zip_code}")
        safe_print(f"📊 Confidence: {confidence}% | Redundancy: {redundancy}%")
        safe_print("")
        
        # Show the separate field values being checked
        if field_values:
            safe_print("📋 Field Values Being Checked:")
            for field_name, field_value in field_values.items():
                if field_value and str(field_value).strip() and str(field_value).lower() != "none":
                    # Check if this field contains a duplicate
                    is_duplicate = duplicates_found and any(dup.startswith(field_name.lower() + ":") for dup in duplicates_found)
                    marker = " 🔄" if is_duplicate else ""
                    safe_print(f"   {field_name.title()}: '{field_value}'{marker}")
            safe_print("")
        
        # Show which specific duplicates were found
        if duplicates_found:
            safe_print("🔄 Duplicates Found in Address:")
            for dup in duplicates_found:
                if ":" in dup:
                    field_type, value = dup.split(":", 1)
                    safe_print(f"   {field_type.title()}: '{value}'")
            safe_print("")
        
        safe_print(f"🔸 ORIGINAL:  '{original}'")
        safe_print(f"🔹 EXTRACTED: '{extracted}'")
        safe_print("")
    
    def get_user_choice(self, original, extracted, city="", state="", zip_code="", confidence=0, redundancy=0, field_values=None, duplicates_found=None):
        """Interactive prompt for user decision"""
        
        # Check if we have a cached decision for similar addresses
        cache_key = f"{original}|{city}|{state}|{zip_code}"
        if cache_key in self.user_decisions:
            return self.user_decisions[cache_key]
        
        # Check if user chose "apply to all"
        if self.apply_to_all and self.global_choice:
            if self.global_choice == 'original':
                return original
            elif self.global_choice == 'extracted':
                return extracted
        
        self.display_comparison(original, extracted, city, state, zip_code, confidence, redundancy, field_values, duplicates_found)
        
        while True:
            print("Options:")
            print("  [1] Keep original address")
            print("  [2] Use extracted address") 
            print("  [3] Enter custom address")
            print("  [4] Apply choice to ALL remaining addresses")
            print("  [s] Skip this address (keep original)")
            print("  [q] Quit processing")
            
            try:
                choice = input("\nYour choice (1-4, s, q): ").strip().lower()
                
                if choice in ['1', 's', '']:
                    result = original
                    break
                elif choice == '2':
                    result = extracted
                    break
                elif choice == '3':
                    while True:
                        custom = input("Enter custom address: ").strip()
                        if custom:
                            result = custom
                            break
                        else:
                            print("Please enter a valid address.")
                    break
                elif choice == '4':
                    # Apply to all
                    print("\nWhat should be applied to ALL remaining addresses?")
                    print("  [1] Keep all originals")
                    print("  [2] Use all extractions")
                    
                    global_choice = input("Choice for all (1 or 2): ").strip()
                    if global_choice == '1':
                        self.apply_to_all = True
                        self.global_choice = 'original'
                        result = original
                        break
                    elif global_choice == '2':
                        self.apply_to_all = True
                        self.global_choice = 'extracted'
                        result = extracted
                        break
                    else:
                        print("Invalid choice. Please try again.")
                        continue
                elif choice == 'q':
                    print("Quitting address processing...")
                    result = original  # Default to original
                    self.apply_to_all = True
                    self.global_choice = 'original'
                    break
                else:
                    print("Invalid choice. Please try again.")
                    continue
                    
            except (KeyboardInterrupt, EOFError):
                print("\n\nInterrupted. Keeping original address...")
                result = original
                break
        
        # Cache the decision
        self.user_decisions[cache_key] = result
        return result

class AddressDeduplicator:
    def __init__(self):
        self.state_mappings = self._load_state_mappings()
        self.abbrev_to_state = {v: k for k, v in self.state_mappings.items()}
    
    def _load_state_mappings(self):
        """Load state mappings from states.yml if available, otherwise use defaults"""
        try:
            if Path("states.yml").exists():
                with open("states.yml") as f:
                    states_data = yaml.safe_load(f)
                if isinstance(states_data, dict):
                    return {k.lower(): v.lower() for k, v in states_data.items()}
            # Fallback to common state mappings
            return {
                'alabama': 'al', 'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar', 'california': 'ca',
                'colorado': 'co', 'connecticut': 'ct', 'delaware': 'de', 'florida': 'fl', 'georgia': 'ga',
                'hawaii': 'hi', 'idaho': 'id', 'illinois': 'il', 'indiana': 'in', 'iowa': 'ia',
                'kansas': 'ks', 'kentucky': 'ky', 'louisiana': 'la', 'maine': 'me', 'maryland': 'md',
                'massachusetts': 'ma', 'michigan': 'mi', 'minnesota': 'mn', 'mississippi': 'ms',
                'missouri': 'mo', 'montana': 'mt', 'nebraska': 'ne', 'nevada': 'nv', 'new hampshire': 'nh',
                'new jersey': 'nj', 'new mexico': 'nm', 'new york': 'ny', 'north carolina': 'nc',
                'north dakota': 'nd', 'ohio': 'oh', 'oklahoma': 'ok', 'oregon': 'or', 'pennsylvania': 'pa',
                'rhode island': 'ri', 'south carolina': 'sc', 'south dakota': 'sd', 'tennessee': 'tn',
                'texas': 'tx', 'utah': 'ut', 'vermont': 'vt', 'virginia': 'va', 'washington': 'wa',
                'west virginia': 'wv', 'wisconsin': 'wi', 'wyoming': 'wy'
            }
        except Exception as e:
            logger.warning(f"Could not load states.yml: {e}. Using defaults.")
            return {}

    def normalize_state(self, state_text: str) -> str:
        """Normalize state to abbreviation format"""
        if not state_text or str(state_text).lower() == "none":
            return ""
        state_clean = str(state_text or "").lower().strip()
        if len(state_clean) == 2 and state_clean in self.abbrev_to_state:
            return state_clean.upper()
        if state_clean in self.state_mappings:
            return self.state_mappings[state_clean].upper()
        return state_text.upper()

    def smart_extract_address(self, address: str, city: str = None, state: str = None, zip_code: str = None):
        """Extract the core address by intelligently removing duplicates"""
        if not address:
            return {"result": "", "confidence": 0, "duplicates": []}
        
        duplicates = []
        remaining = address.strip()
        confidence = 100
        
        # Normalize inputs (convert to strings first)
        city_norm = str(city or "").lower().strip() if city else ""
        state_norm = self.normalize_state(str(state or "")) if state else ""
        zip_norm = str(zip_code or "").strip() if zip_code else ""
        
        # Remove zip (most reliable)
        if zip_norm and len(zip_norm) >= 4:
            pattern = rf'\b{re.escape(zip_norm)}\b'
            if re.search(pattern, remaining):
                remaining = re.sub(pattern, '', remaining, flags=re.IGNORECASE)
                duplicates.append(f"zip:{zip_norm}")
        
        # Remove state
        if state_norm:
            state_pattern = rf'\b{re.escape(state_norm)}\b'
            if re.search(state_pattern, remaining, flags=re.IGNORECASE):
                remaining = re.sub(state_pattern, '', remaining, flags=re.IGNORECASE)
                duplicates.append(f"state:{state_norm}")
            elif state_norm.lower() in self.abbrev_to_state:
                full_state = self.abbrev_to_state[state_norm.lower()].title()
                full_pattern = rf'\b{re.escape(full_state)}\b'
                if re.search(full_pattern, remaining, flags=re.IGNORECASE):
                    remaining = re.sub(full_pattern, '', remaining, flags=re.IGNORECASE)
                    duplicates.append(f"state:{full_state}")
        
        # Remove city (careful - could be part of street name)
        if city_norm and len(city_norm) > 2:
            city_pattern = rf'\b{re.escape(city_norm)}\b'
            matches = list(re.finditer(city_pattern, remaining, flags=re.IGNORECASE))
            if matches:
                last_match = matches[-1]
                remaining = remaining[:last_match.start()] + remaining[last_match.end():]
                duplicates.append(f"city:{city}")
                if len(city_norm) < 5:
                    confidence -= 20
        
        # Clean up result
        result = re.sub(r'[,\s]+', ' ', remaining).strip(' ,.')
        
        # Adjust confidence
        if len(duplicates) == 0:
            confidence = 10
        elif len(result) < 5:
            confidence = 30
        
        return {
            "result": result,
            "confidence": confidence,
            "duplicates": duplicates
        }

def smart_address_deduplication(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    """
    SMART ADDRESS DEDUPLICATION with Interactive Mode
    
    Configuration options:
    - smart_address_dedup: list of fields to check for duplicates  
    - dedup_action: "extract", "flag_only", "interactive", or "score_only"
    - confidence_threshold: minimum confidence to auto-extract (default: 70)
    - redundancy_threshold: minimum redundancy % to flag (default: 10)
    """
    remove_fields = field.get("smart_address_dedup", [])
    action = field.get("dedup_action", "extract")
    confidence_threshold = field.get("confidence_threshold", 70)
    redundancy_threshold = field.get("redundancy_threshold", 10)
    
    if not remove_fields:
        return df
    
    alias = field.get("alias")
    source = field.get("source_name")
    field_name = alias if alias else source
    
    if field_name not in df.columns:
        return df
    
    valid_fields = [f for f in remove_fields if f in df.columns]
    if not valid_fields:
        return df

    def map_address_fields(field_names):
        """Intelligently map field names to address components"""
        mapping = {'city': None, 'state': None, 'zip': None}
        
        for field_name in field_names:
            field_lower = field_name.lower()
            
            # State patterns (check first to avoid conflicts with 'postal_code' containing 'code')
            if any(pattern in field_lower for pattern in ['state', 'province', 'region']) or \
               field_lower in ['st', 'prov'] or \
               (field_lower.endswith('_st') or field_lower.startswith('st_')) or \
               ('state' in field_lower and 'code' in field_lower):  # e.g., 'state_code'
                mapping['state'] = field_name
            
            # Zip patterns (more specific to avoid conflicts)
            elif any(pattern in field_lower for pattern in ['zip', 'postal']) or \
                 field_lower in ['code', 'postcode'] or \
                 (field_lower.endswith('code') and 'state' not in field_lower):
                mapping['zip'] = field_name
            
            # City patterns (check last as they're most general)
            elif any(pattern in field_lower for pattern in ['city', 'town', 'municipality', 'lugar']):
                mapping['city'] = field_name
        
        return mapping
    
    # Get dynamic field mapping
    field_mapping = map_address_fields(valid_fields)
    
    deduplicator = AddressDeduplicator()
    
    # Collect addresses for interactive review (only if action is "interactive")
    addresses_for_review = []
    decisions = {}
    
    # First pass: identify addresses that need review
    for row in df.iter_rows(named=True):
        address = row[field_name]
        if not isinstance(address, str):
            continue
            
        # Get field values dynamically using the mapping
        city = ""
        state = ""
        zip_code = ""
        
        if field_mapping['city'] and field_mapping['city'] in row:
            city = str(row[field_mapping['city']] or "")
        
        if field_mapping['state'] and field_mapping['state'] in row:
            state = str(row[field_mapping['state']] or "")
            
        if field_mapping['zip'] and field_mapping['zip'] in row:
            zip_code = str(row[field_mapping['zip']] or "")
        
        # Check for duplicates
        extraction = deduplicator.smart_extract_address(address, city, state, zip_code)
        total_chars = len(address)
        duplicate_chars = sum(len(dup.split(":", 1)[1]) for dup in extraction["duplicates"] if ":" in dup)
        redundancy_score = round((duplicate_chars / total_chars) * 100, 1) if total_chars > 0 else 0
        
        # Check if needs interactive review (only for "interactive" action)
        if (action == "interactive" and 
            extraction["duplicates"] and 
            redundancy_score >= redundancy_threshold):
            
            # Store all field values for display
            field_display_values = {}
            for field_name_key in valid_fields:
                field_display_values[field_name_key.lower()] = str(row.get(field_name_key, "") or "")
            
            addresses_for_review.append({
                "address": address,
                "extracted": extraction["result"],
                "city": city,
                "state": state,
                "zip": zip_code,
                "confidence": extraction["confidence"],
                "redundancy": redundancy_score,
                "field_values": field_display_values,
                "duplicates": extraction["duplicates"]
            })
    
    # Interactive review process (only if action is "interactive")
    if addresses_for_review and action == "interactive":
        safe_print(f"\n🔍 Found {len(addresses_for_review)} addresses with potential duplicates.")
        safe_print("Starting interactive review...\n")
        
        processor = InteractiveAddressProcessor()
        
        for i, addr_data in enumerate(addresses_for_review):
            safe_print(f"\nProgress: {i+1}/{len(addresses_for_review)}")
            
            # Prepare field values for display
            display_fields = addr_data.get("field_values", {})
            
            decision = processor.get_user_choice(
                addr_data["address"],
                addr_data["extracted"], 
                addr_data["city"],
                addr_data["state"],
                addr_data["zip"],
                addr_data["confidence"],
                addr_data["redundancy"],
                display_fields,
                addr_data.get("duplicates", [])
            )
            
            decisions[addr_data["address"]] = decision
        
        safe_print(f"\n✅ Interactive review completed! Processed {len(decisions)} addresses.")
    
    # Apply decisions
    def process_address(row):
        address = row[field_name]
        if not isinstance(address, str):
            return address
        
        # Check for interactive decision first
        if address in decisions:
            return decisions[address]
        
        # Process based on action type - use dynamic field mapping here too
        city = ""
        state = ""
        zip_code = ""
        
        if field_mapping['city'] and field_mapping['city'] in row:
            city = str(row[field_mapping['city']] or "")
        
        if field_mapping['state'] and field_mapping['state'] in row:
            state = str(row[field_mapping['state']] or "")
            
        if field_mapping['zip'] and field_mapping['zip'] in row:
            zip_code = str(row[field_mapping['zip']] or "")
        
        extraction = deduplicator.smart_extract_address(address, city, state, zip_code)
        total_chars = len(address)
        duplicate_chars = sum(len(dup.split(":", 1)[1]) for dup in extraction["duplicates"] if ":" in dup)
        redundancy_score = round((duplicate_chars / total_chars) * 100, 1) if total_chars > 0 else 0
        
        if action == "score_only":
            # Just analyze, don't modify
            return address
            
        elif action == "flag_only":
            # Flag duplicates for manual review
            if extraction["duplicates"] and redundancy_score >= redundancy_threshold:
                return f"%%%%{address}%%%%"
            return address
            
        elif action in ["extract", "interactive"]:
            # Smart extraction (auto for "extract", fallback for "interactive")
            if extraction["confidence"] >= confidence_threshold and redundancy_score >= redundancy_threshold:
                return extraction["result"]
            elif redundancy_score >= redundancy_threshold:
                return f"%%%%{address}%%%% [SUGGEST: {extraction['result']}]"
            else:
                return address
        
        return address
    
    # Apply processing to each row
    processed_addresses = []
    for row in df.iter_rows(named=True):
        processed_address = process_address(row)
        processed_addresses.append(processed_address)
    
    # Update the DataFrame with processed addresses
    df = df.with_columns(
        pl.Series(processed_addresses).alias(field_name)
    )
    
    return df

def do_explore(conifg, df):
    pass

def fix_nested_fields(config: dict, df: pl.DataFrame, target_file) -> pl.DataFrame:
    target_file = Path(target_file)
    csv_file_name = target_file.stem + ".csv"
    fields_in_field = config.get("fields_in_field", [])
    for explode in fields_in_field:
            delimiter = explode.get("delimiter", "|")
            splitter = explode.get("key_value_splitter", ":")
            field_name = explode.get("field_name", "Custom Fields")
            ignore_junk_field_string = explode.get("ignore_junk_field_string", ["n"])
            df = explode_custom_fields(df, field_name, delimiter, splitter, ignore_junk_field_string)
    if len(fields_in_field) > 0:
        create_output_file(csv_file_name, df)
    return df


def explode_custom_fields(df: pl.DataFrame, on_field: str, delimiter: str, splitter: str, ignore_junk_fields: list = ['n']) -> pl.DataFrame:
    df = df.with_columns(pl.arange(0, df.height).cast(pl.datatypes.Utf8).alias("row_number"))
    if on_field in df.columns:
        # Split the "Custom Fields" column by '|'
        custom_fields = df[on_field].map_elements(lambda x: x.split(delimiter) if isinstance(x, str) else x, return_dtype=pl.datatypes.List)
        #remove field if field is 'n'
        for ignore in ignore_junk_fields:
            custom_fields = custom_fields.map_elements(lambda x: [field for field in x if field != ignore], return_dtype=pl.datatypes.List)
        logger.info(custom_fields)
        # For each split column
        row_number = 0
        for field in custom_fields:
            # Split the column by ':'
            split_column = field.map_elements(lambda x: x.split(splitter), return_dtype=pl.datatypes.List)
            field_name = split_column.map_elements(lambda x: x[0].strip() if len(x) > 1 else "", return_dtype=pl.datatypes.Utf8)
            field_value = split_column.map_elements(lambda x: x[1].strip() if len(x) > 1 else "", return_dtype=pl.datatypes.Utf8)
            field_name = field_name.append(pl.Series(["row_number"]))
            field_value = field_value.append(pl.Series([str(row_number)]))
            if "df_exploded_fields" not in locals():
                df_exploded_fields = pl.DataFrame({field_name[i]: [field_value[i]] for i in range(len(field_name))})
            else:
                tmp = pl.DataFrame({field_name[i]: [field_value[i]] for i in range(len(field_name))})
                df_exploded_fields = pl.concat([df_exploded_fields, tmp], how='align')
            row_number += 1
        # Merge the exploded fields with the original DataFrame on the row number
        df = df.join(df_exploded_fields, on="row_number", how="inner")
        # Drop the "Custom Fields" column
        df = df.drop(on_field)
        # order by row number asc and drop row number
        df = df.sort(pl.col("row_number").cast(pl.Int64))#.drop("row_number")
        # write_path = "output.csv"
        # df.write_csv(write_path)
        #show the last 5 rows
        logger.info("printing with row_number")
        
    else: 
        raise ValueError(f"{field_name} not found in the DataFrame.")
    #
    return df

def rename_columns(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    alias = field.get("alias")
    source = field.get("source_name")

    if not alias or not source:
        return df
    if source not in df.columns:
        logger.debug(f"{source} not found in the DataFrame. Will not rename")
        return df
    
    #rename source field to alias
    logger.info(f"will rename {source} to {alias}")
    df = df.rename({source: alias})
    
    return df

# def replace_strings_with_file(df: pl.DataFrame, field: dict) -> pl.DataFrame:
#     with open(filename) as f:
#         data = yaml.safe_load(f)
#     field["replace"] = data
#     return replace_strings(df, field)

def replace_strings(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    replace = field.get("replace", [])
    replace_filename = field.get("replace_with_file")
    
    if (not source and not alias) or not (replace or replace_filename):
        logger.debug(field)
        logger.debug("source or replace not found in the field.")
        return df
    if alias and alias not in df.columns:
        logger.debug(f"{alias} not found in the DataFrame.")
        return df
    
    if not alias and source not in df.columns:
        logger.info(f"{source} not found in the DataFrame.")
        return df
    if replace_filename:
        with open(replace_filename) as f:
            data = yaml.safe_load(f)
        replace = replace + data
    if alias:
        field_name = alias
    else:
        field_name = source 

    for item in replace:
        from_string=item.get("from")
        to_string=item.get("to")
        is_expression = item.get("is_expression")
        is_literal = False if is_expression == True else True #flip it for ease of use in the config
        logger.info(f"setting is_literal to {is_literal} for {from_string} to {to_string} in {field_name}")
        logger.info(rf"will replace {from_string} with {to_string} in {field_name}")
        df = df.with_columns(pl.col(field_name).str.replace_all(from_string, to_string, literal=is_literal))
        #df = df.with_columns(pl.col(field_name).str.replace_all("[Dd]r($| )", "Dr.$1", literal=False))
        
    return df

def fix_casing(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    casing = field.get("casing")
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    if not casing:
        logger.debug(f"casing not required for {field_name}.")
        return df
    
    if casing == "upper":
        logger.info(f"will convert {field_name} to uppercase.")
        df = df.with_columns(pl.col(field_name).str.to_uppercase())
    elif casing == "lower":
        df = df.with_columns(pl.col(field_name).str.to_lowercase())
    elif casing == "proper":
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
    elif casing == "name":
        #ex) john doe -> John Doe
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
        #find Mc or Mac and capitalize the next letter
        df = df.with_columns(pl.col(field_name).apply(lambda value: re.sub(r"(Mc)(\w)", _name_replacement, value)))
        #capitalize letter after ' and -
        df = df.with_columns(pl.col(field_name).apply(lambda value: re.sub(r"('|\-)(\w)", _name_replacement, value)))
        #if the field starts with a single letter followed by a space, and the length of the field is more than 2, remove the first letter and the space
        df = df.with_columns(pl.when(pl.col(field_name).str.len_chars() > 2).then(
            pl.col(field_name).str.replace_all(r"(^\w\s)", "$2")
        ).otherwise(
            pl.col(field_name)
        ))
    else:    
        logger.info(f"{casing} not supported.")
    
    return df
def _name_replacement(match):
    return match.group(1) + match.group(2).capitalize()

def validate_against_list(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    in_list = field.get("in_list")

    field_name = alias if alias else source

    if not in_list:
        logger.debug(f"in_list not found in the {field_name}. will skip validation.")
        return df
    with open(in_list) as f:
        list_yaml = yaml.safe_load(f)

    logger.info(f"will validate {field_name} against {in_list}")
    df = df.with_columns(pl.col(field_name).apply(lambda x: f'%%%%{x}%%%%' if x not in list_yaml else x))
    
    return df
def remove_duplicates_from_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    """
    Smart Address Deduplication Function
    
    This function intelligently removes duplicate address components (city, state, zip) 
    from address fields based on separate field values.
    
    Configuration:
    - smart_address_dedup: list of fields to check for duplicates (e.g., ["City", "State", "Zip"])
    - dedup_action: how to handle duplicates
        * "extract": automatically remove duplicates with high confidence
        * "flag_only": mark addresses with duplicates using %%%% markers
        * "interactive": prompt user for decisions on each flagged address
        * "score_only": analyze only, no modifications
    - confidence_threshold: minimum confidence for auto-extraction (default: 70)
    - redundancy_threshold: minimum redundancy % to flag addresses (default: 10)
    """
    # Check for smart address deduplication parameters
    smart_fields = field.get("smart_address_dedup", [])
    dedup_action = field.get("dedup_action", None)
    
    # Only proceed if using smart address deduplication
    if smart_fields and dedup_action:
        return smart_address_deduplication(df, field)
    
    # Return unchanged if no smart deduplication configured
    return df

def select_columns(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    select_fields = field.get("ordered_headers")
    select_fields.extend(["legoberry_drop_field_indicator", "legoberry_reason_for_drop"])
    logger.info(select_fields)
    if not select_fields:
        logger.info(f"{select_fields} not found. Will pass all fields")
        return df
    logger.info(f"will select {select_fields} from the DataFrame.")
    #create field if not found
    for field in select_fields:
        if field not in df.columns:
            df = df.with_columns(pl.Series([None]).alias(field))
    df = df.select(select_fields)
    
    return df

def create_new_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    alias = field.get("alias")
    field_name = alias if alias else field.get("source_name")
    default_value = field.get("default_value")
    source_fields_alpha_only = field.get("source_fields_alpha_only")
    allow_special_char_list = field.get("allow_special_char_list", [])
    if not alias:
        logger.info(f"alias not found for {field_name}. Will skip creating new field.")
        return df
    if alias in df.columns:
        logger.info(f"{alias} already exists in the DataFrame. Will not alias a new created field")
        return df
    if not default_value:
        logger.debug(f"{default_value} not found in the field.")
        return df

    logger.info(f"will create {alias} with default value {default_value}")
    if  "{" in default_value and "}" in default_value: 
        split_values = re.split(r'[{}]', default_value)
        allowed_chars = ''.join(re.escape(char) for char in allow_special_char_list)
        if source_fields_alpha_only:
            replace = rf'[^a-zA-Z{allowed_chars}]'
            #if p.col add replace_all function
            split_values = [f"pl.col('{value.strip()}').str.replace_all('{replace}', '')" if i % 2 == 1 else f"'{value}'" for i, value in enumerate(split_values) if value.strip()]        # Join the list back into a string
        else:
            split_values = [f"pl.col('{value.strip()}')" if i % 2 == 1 else f"'{value}'" for i, value in enumerate(split_values) if value.strip()]        # Join the list back into a string
        default_value = "+".join(split_values)
        logger.info(default_value)
        df = df.with_columns(eval(default_value).alias(alias))
        return df
    df = df.with_columns(pl.Series([default_value]).alias(alias))
    return df

def format_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    data_type = field.get("data_type")
    data_format = field.get("data_format")
    reformat_to = field.get("reformat_to")
    if not data_type:
        logger.info(f'data_type not found in the "{field_name}" field. Will skip formatting.')
        return df
    failed_records = pl.DataFrame()
    data_type = data_type.lower()

    if data_type == "date":
        if not data_format:
            logger.info(f'date not found in the "{field_name}" field. Will skip formatting to date.')
            return df
        parsed_date = pl.col(field_name).str.to_date(data_format, strict=False)
        if reformat_to:
            parsed = parsed_date.dt.strftime(reformat_to)
        else:
            parsed = parsed_date
        df = df.with_columns(
            pl.when(pl.col(field_name) == "")
            .then(None)
            .when(parsed.is_not_null())
            .then(parsed)
            .otherwise("%%%%" + pl.col(field_name) + "%%%%")
            .alias(field_name)
        )
    elif data_type == "integer":
        logger.info(f"will convert {field_name} to integer.")
        #try to convert to integer, if fails, add *** to the field and keep it as string
        df = df.with_columns(
            pl.when(pl.col(field_name).str.contains(r'^[0-9]+$'))
            .then(pl.col(field_name))
            .otherwise(pl.lit(None))
            .alias(field_name)
        )

        df = df.with_columns(pl.col(field_name).cast(pl.Int64))
        
    elif data_type == "number":
        logger.info(f"will convert {field_name} to number.")
        df = df.with_columns(pl.col(field_name).cast(pl.Float64))
    elif data_type == "string":
        logger.info(f"will convert {field_name} to string.")
        df = df.with_columns(pl.col(field_name).cast(pl.Utf8))
    elif data_type == "boolean":
        logger.info(f"will convert {field_name} to boolean.")
        df = df.with_columns(pl.col(field_name).cast(pl.Boolean))
    elif data_type == "phone number":
        logger.info(f"will convert {field_name} to phone number.")
        #ex) (123) 456-7890 or 123-456-7890 or 123.456.7890 or 1234567890 -> (123) 456-7890
        df = df.with_columns(pl.col(field_name).str.replace_all(r"[^\d]", ""))
        # df = df.with_columns(
        #     "(" + pl.col(field_name).str.slice(0, 3) + ") " 
        #     + pl.col(field_name).str.slice(3, 3) 
        #     + "-" + pl.col(field_name).str.slice(6, 4)
        #     .alias(field_name))
        df = _format_phone_numbers(df, field_name)

    elif data_type == "zip code":
        logger.info(f"will convert {field_name} to zip code.")
        #ex) 12345-6789 or 12345 -> 12345
        #ex) 2345 -> 02345
        df = df.with_columns(pl.col(field_name).cast(pl.Utf8))
        condition = pl.col(field_name).str.len_chars() == 4
        warning_condition = pl.col(field_name).str.len_chars() < 4
        formatted = pl.when(condition).then(
            pl.col(field_name).str.zfill(5)
        ).when(warning_condition).then(
            "%%%%" + pl.col(field_name) + "%%%%"
        ).otherwise(
            pl.col(field_name).str.slice(0, 5)
        )
        df = df.with_columns(formatted.alias(field_name))

                             
    elif data_type == "email":
        logger.info(f"will convert {field_name} to email.")
        regex_valid_email = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        df = df.with_columns(
            pl.when(pl.col(field_name).str.contains(regex_valid_email, literal=False) & pl.col(field_name).is_not_null())
            .then(pl.col(field_name)).when(pl.col(field_name) == "").then(None)
            .otherwise("%%%%" + pl.col(field_name) + "%%%%")
        )
    else:
        logger.info(f"{data_type} not supported.")
    
    return df

def _format_phone_numbers(df, field_name):
    old_field_name = f"{field_name}_old"
    df = df.rename({field_name: old_field_name})
    # Apply formatting if the condition is met
    condition = pl.col(old_field_name).str.len_chars() == 10
    # if phone number is 11 digits long and starts with 1, remove 1
    eleven_digits = pl.col(old_field_name).str.len_chars() == 11
    formatted = pl.when(condition).then(
        "(" + pl.col(old_field_name).str.slice(0, 3) + 
        ") " + pl.col(old_field_name).str.slice(3, 3) + 
        "-" + pl.col(old_field_name).str.slice(6, 4)
    ).when(eleven_digits).then(
        "(" + pl.col(old_field_name).str.slice(1, 3) + 
        ") " + pl.col(old_field_name).str.slice(4, 3) + 
        "-" + pl.col(old_field_name).str.slice(7, 4)
    ).otherwise(
        "%%%%" + pl.col(old_field_name) + "%%%%" # Return unchanged if not exactly 10 digits
    )
    logger.info(field_name)
    return df.with_columns(formatted.alias(field_name))
    
def get_excel_formats(output_config: dict) -> dict:
    column_formats = {}
    for field in output_config.get("fields"):
        alias = field.get("alias")
        source = field.get("source_name")
        field_name = alias if alias else source
        format = field.get("excel_format")
        if format:
            column_formats[field_name] = format
    return column_formats

def drop_duplicates(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    fields_list = config.get("fields_to_consider_duplicates")
    if not fields_list:
        logger.info(f"{fields_list} not found in the config. No fields to consider for duplicates.")
        return df
    logger.info(f"will drop duplicates based on {fields_list}")
    df = df.unique(subset=fields_list)
    return df

def truncate_max_length(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    max_length = field.get("max_length")

    if not max_length:
        logger.debug(f"Will not format truncate {field_name}.")
        return df
    logger.info(f"will truncate {field_name} to {max_length}")
    #chceck data type of the field
    if df[field_name].dtype == pl.datatypes.Utf8:
        df = df.with_columns(pl.col(field_name).str.slice(0, max_length))
    else:
        logger.info(f"{field_name} is not of type Utf8.")
        data_type = df[field_name].dtype
        df = df.with_columns(pl.col(field_name).cast(pl.datatypes.Utf8).str.slice(0, max_length))
        df = df.with_columns(pl.col(field_name).cast(data_type))
    
    return df

def create_record_off_field(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    field_name = field.get("source_name")
    expand_on = field.get("expand_on")

    if not expand_on:
        logger.debug(f"Will not format expand on {field_name or field.get('alias')}.")
        return df
    logger.info(f"will create record off {field_name} on {expand_on}")
    df_alt = df.clone()
    #remove row if field is empty or null
    df_alt = df_alt.filter(pl.col(field_name).is_not_null())
    df_alt = df_alt.drop([expand_on])
    df_alt = df_alt.with_columns(pl.col(field_name).alias(expand_on))
    df = pl.concat([df, df_alt], how='align')

    return df

def drop_nulls(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    drop_full_row_if_empty = field.get("drop_full_row_if_empty")
    if not drop_full_row_if_empty:
        logger.debug(f"Will not format drop full row if {field.get('source_name') or field.get('alias')} is null.")
        return df
    source = field.get("source_name")
    alias = field.get("expand_on") or field.get("alias")
    field_name = alias if alias else source
    logger.info(f"will drop nulls from {field_name}")
    #set legoberry_drop_field_indicator to true if field is null
    if "legoberry_drop_field_indicator" in df.columns:
        #add to existing drop field indicator if exists
        df = df.with_columns(pl.when((pl.col(field_name).is_null()) | (pl.col(field_name) == "")).then(
            True
        ).otherwise(
            pl.col("legoberry_drop_field_indicator")
        ).alias("legoberry_drop_field_indicator"))
    else:
        df = df.with_columns(pl.when((pl.col(field_name).is_null()) | (pl.col(field_name) == "")).then(
            True
        ).otherwise(False).alias("legoberry_drop_field_indicator"))

    if "legoberry_reason_for_drop" in df.columns:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True & pl.col("legoberry_reason_for_drop").is_null()).then(
            pl.lit(f"{source} is Null")
        ).otherwise(
            pl.col("legoberry_reason_for_drop")
        ).alias("legoberry_reason_for_drop"))
    else:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True).then(
            pl.lit(f"{source} is Null")
        ).alias("legoberry_reason_for_drop"))


    #df = df.filter(pl.col(field_name).is_not_null())
    #df = df.filter(pl.col(field_name).str.contains("%%%%%%%%", literal=True).is_not())
    return df

def drop_if_length_less_than(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    drop_if_length_less_than=field.get("drop_if_length_less_than")
    if not drop_if_length_less_than:
        logger.debug(f"Will not format check for length of {field.get('source_name')}.")
        return df
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    logger.info(f"will drop if length of {field_name} is less than {drop_if_length_less_than}")
    if "legoberry_drop_field_indicator" in df.columns:
        df = df.with_columns(
            pl.when(pl.col(field_name).str.len_chars() < drop_if_length_less_than).then(
                True
            ).otherwise(
                pl.col("legoberry_drop_field_indicator")
            ).alias("legoberry_drop_field_indicator")
        )
    else:
        df = df.with_columns(
            pl.when(pl.col(field_name).str.len_chars() < drop_if_length_less_than).then(
                True
            ).otherwise(False).alias("legoberry_drop_field_indicator")
        )

    if "legoberry_reason_for_drop" in df.columns:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True & pl.col("legoberry_reason_for_drop").is_null()).then(
            pl.lit(f"Length of {field_name} is less than {drop_if_length_less_than}")
        ).otherwise(
            pl.col("legoberry_reason_for_drop")
        ).alias("legoberry_reason_for_drop"))
    else:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True).then(
            pl.lit(f"Length of {field_name} is less than {drop_if_length_less_than}")
        ).alias("legoberry_reason_for_drop"))
    
    return df