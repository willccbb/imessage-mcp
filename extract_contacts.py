import sqlite3
import os
import re
import polars as pl

def extract_contacts_from_abcddb(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Map to hold contact information by record ID
    contacts = {}

    # Fetch contact names and IDs
    cursor.execute("""
        SELECT ZABCDRECORD.Z_PK, ZABCDRECORD.ZFIRSTNAME, ZABCDRECORD.ZMIDDLENAME, ZABCDRECORD.ZLASTNAME, ZABCDRECORD.ZORGANIZATION, ZABCDRECORD.ZNOTE
        FROM ZABCDRECORD
    """)

    for row in cursor.fetchall():
        record_id = row[0]
        first_name = row[1] or ''
        middle_name = row[2] or ''
        last_name = row[3] or ''
        organization = row[4] or ''
        note = row[5] or ''

        full_name = ' '.join([first_name, middle_name, last_name]).strip()
        contacts[record_id] = {
            'Name': full_name if full_name else organization,
            'First Name': first_name,
            'Middle Name': middle_name,
            'Last Name': last_name,
            'Organization': organization,
            'Note': note,
            'Phone Numbers': [],
            'Emails': []
        }

    # Fetch phone numbers
    cursor.execute("""
        SELECT ZABCDPHONENUMBER.ZOWNER, ZABCDPHONENUMBER.ZFULLNUMBER
        FROM ZABCDPHONENUMBER
    """)

    for row in cursor.fetchall():
        owner_id = row[0]
        phone_number = row[1]
        if owner_id in contacts and phone_number:
            contacts[owner_id]['Phone Numbers'].append(phone_number)

    # Fetch email addresses
    cursor.execute("""
        SELECT ZABCDEMAILADDRESS.ZOWNER, ZABCDEMAILADDRESS.ZADDRESS
        FROM ZABCDEMAILADDRESS
    """)

    for row in cursor.fetchall():
        owner_id = row[0]
        email = row[1]
        if owner_id in contacts and email:
            contacts[owner_id]['Emails'].append(email)

    conn.close()

    # Create a simplified contacts list
    simplified_contacts = []
    for contact in contacts.values():
        name_fields = {
            'Name': contact['Name'],
            'First Name': contact['First Name'],
            'Middle Name': contact['Middle Name'],
            'Last Name': contact['Last Name'],
            'Organization': contact['Organization'],
            'Note': contact['Note'],
        }
        # For each phone number, create a separate entry
        for phone in contact['Phone Numbers']:
            # Clean phone number formatting
            cleaned_phone = re.sub(r'[^\d+]', '', phone)
            if not cleaned_phone.startswith('+'):
                cleaned_phone = '+1' + cleaned_phone.lstrip('1')
            entry = name_fields.copy()
            entry['Phone Number'] = cleaned_phone
            entry['Email'] = None
            simplified_contacts.append(entry)
        # For each email address, create a separate entry
        for email in contact['Emails']:
            entry = name_fields.copy()
            entry['Phone Number'] = None
            entry['Email'] = email
            simplified_contacts.append(entry)

    return simplified_contacts

def extract_contacts(abbu_dir):
    # Find all .abcddb files within the abbu directory
    abcddb_files = []
    for root, dirs, files in os.walk(abbu_dir):
        for file in files:
            if file.endswith('.abcddb'):
                abcddb_files.append(os.path.join(root, file))

    if not abcddb_files:
        print(f"No .abcddb files found in {abbu_dir}")
        return pl.DataFrame()

    contacts = []

    for db_file in abcddb_files:
        # print(f"Processing {db_file}")
        contacts.extend(extract_contacts_from_abcddb(db_file))

    if not contacts:
        print("No contacts found.")
        return pl.DataFrame()

    # Create a Polars DataFrame
    df = pl.DataFrame(contacts, infer_schema_length=10_000)

    # Remove entries without phone number or email
    df = df.filter(
        (pl.col('Phone Number').is_not_null()) | (pl.col('Email').is_not_null())
    )

    return df

# Usage
# if __name__ == '__main__':
#     df = extract_contacts('contacts.abbu')
#     print(df.head())