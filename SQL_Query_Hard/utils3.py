import re

def process_sql_columns(query):
    selected_columns_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.DOTALL)
    if selected_columns_match:
        columns_text = selected_columns_match.group(1).strip()
        # Normalize spaces and handle line transitions within the query
        columns_text = re.sub(r'\s*\n\s*', ', ', columns_text)
        columns_text = re.sub(r'[ \t]+', ' ', columns_text)
        # Splitting the columns, ensuring we don't split inside brackets
        columns = re.split(r',(?![^\(\[]*[\]\)])', columns_text)

        parsed_columns = []
        for column in columns:
            column = column.strip()
            # Remove substrings starting with '%' using a regular expression
            column = re.sub(r'%\S+', '', column)
            if ' AS ' in column:
                parts = re.split(r'\s+AS\s+', column)
                column_name = parts[0].strip()
                alias = parts[1].strip() if len(parts) > 1 else column_name
                parsed_columns.append((column_name, alias))
            else:
                parsed_columns.append((column, column))

        updated_columns = []
        for column_name, alias in parsed_columns:
            text = column_name
            # This while loop continues until there are no more innermost parentheses
            while True:
                innermost_texts = re.findall(r'\(([^()]*)\)', text)
                if not innermost_texts:
                    break
                # Extracting the first part of any comma-separated values inside the innermost parentheses
                names = [item.split(',')[0].strip().replace("'", "").replace("$", "").split(' as ')[0] for item in innermost_texts]
                print(names)
                unique_names = [name for name in set(names) if not name.startswith('%')]
                for innermost_text in innermost_texts:
                    # Replace the whole parenthetical content with simplified names
                    text = re.sub(r'.*\(([^()]*)\).*', r', '.join(unique_names), text)
            updated_columns.append((text, alias))
    return updated_columns
'''
        # Display the updated columns for review
        for text, alias in updated_columns:
            print(f"Column: {text}, Alias: {alias}")
    else:
        print("No SELECT-FROM clause found in the query.")
'''

import re

def extract_sql_details(sql_text):
    # Capture both CROSS JOIN and LEFT OUTER JOIN sections, including any subsequent conditions
    joins = re.findall(r'(CROSS JOIN|LEFT OUTER JOIN)\s+(.*?)(?=\s+LEFT OUTER JOIN|\s+CROSS JOIN|$)', sql_text, re.DOTALL)

    # Dictionary to track table names associated with aliases for substitution
    alias_table_map = {}
    results = []  # Collection to hold results instead of printing them

    for join_type, content in joins:
        if join_type == 'CROSS JOIN':
            # Extract aliases typically following 'AS' keyword for CROSS JOIN
            aliases = re.findall(r'AS\s+(\w+)', content)
            columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', content, re.DOTALL)

        elif join_type == 'LEFT OUTER JOIN':
            # Extract tables and aliases for LEFT OUTER JOIN
            aliases = re.findall(r'<=\s+(\w+)', content)
            columns = re.findall(r'ON\s+(.*?)(?=\s+LEFT OUTER JOIN|\s+CROSS JOIN|\s+WHERE|$)', content, re.DOTALL)
            pattern = r'LEFT OUTER JOIN\s+([^\s]+)\s+AS\s+([^\s]+)\s+ON'
            joins = re.findall(pattern, sql_text, re.IGNORECASE)

        # Process each alias and the respective SQL segment
        for alias, column in zip(aliases, columns):
            table_aliases = re.findall(r'LEFT OUTER JOIN\s+([^\s]+)\s+AS\s+([^\s]+)\s+ON', sql_text)
            table_table = table_aliases[0][0]
            alias_table = table_aliases[0][1]

            if column:
                unique_values = set(re.findall(r'\(([^()]*)\)', column))
                processed_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
                updated_values = []

                for value in processed_values:
                    # Perform the replacement in each string individually
                    prefix = value.split('.')[0] if '.' in value else None
                    if prefix and prefix == alias_table:
                        prefix_table = table_table
                        value = value.replace(alias_table, prefix_table, 1)  # Replace only the first occurrence in each string
                    updated_values.append(value)

                results.append({"Alias": alias, "Processed Values": list(set(updated_values))})  # Deduplicate the final list

    return results

