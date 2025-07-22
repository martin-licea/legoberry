# 0.0.12
## Smart Address Deduplication Feature

### New Features
- **Smart Address Deduplication System**: Intelligently identifies and removes redundant information from address fields by analyzing related city, state, and zip code data
- **Dynamic Field Mapping**: Automatically recognizes various field naming conventions for city, state, and zip fields (e.g., `city`, `town`, `municipality`, `st`, `state`, `zip`, `postal`, etc.)
- **Four Processing Modes**:
  - `extract`: Automatically removes detected duplicates when confidence and redundancy thresholds are met
  - `flag_only`: Marks addresses with duplicates for manual review without modifying them
  - `interactive`: Presents each qualifying address for user review with multiple options
  - `score_only`: Analyzes addresses without making any modifications (for testing/analysis)
- **Interactive Review Interface**: Comprehensive user interface showing confidence scores, redundancy analysis, context display, and batch processing options
- **State Normalization**: Converts between full state names and abbreviations for consistent matching (e.g., "California" ↔ "CA")
- **Confidence & Redundancy Scoring**: Provides metrics to assess extraction reliability and duplicate content percentage
- **Cross-Platform Unicode Support**: Handles emojis and special characters across Windows, macOS, and Linux with automatic fallback

### Configuration
- **New Parameters**:
  - `smart_address_dedup`: List of field names to check for duplicates
  - `dedup_action`: Processing mode (`extract`, `flag_only`, `interactive`, `score_only`)
  - `confidence_threshold`: Minimum confidence % for auto-extraction (default: 70%)
  - `redundancy_threshold`: Minimum redundancy % to trigger processing (default: 10%)

### Improvements
- **Enhanced Address Processing**: Replaces simple string matching with intelligent component analysis
- **Flexible Field Detection**: Dynamic mapping vs. hardcoded field names
- **User Control**: Interactive mode allows manual review and decision-making
- **Performance**: Efficient single-pass analysis with memory optimization
- **Error Handling**: Graceful degradation with type safety and validation

### Migration
- Legacy `remove_duplicates_from_fields` and `mark_duplicates_only` configurations can be migrated to the new `smart_address_dedup` system
- Backward compatibility maintained during transition period

### Technical Details
- **Cross-Platform Compatibility**: Works across Windows, macOS, and Linux
- **Unicode Safe**: Automatic encoding fallback for terminal environments
- **Scalable**: Handles datasets with hundreds of thousands of records
- **Type Safety**: Handles mixed data types (strings, integers, nulls)

# 0.0.8
- Introduce `allow_special_char_list` which allows characters to be included when constructing a new field with a `default`. 