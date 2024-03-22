from lxml import etree as ET
import contextlib
from contextlib import contextmanager
import sys
import csv
import pyodbc
import os
import threading
import timeit

tic = timeit.default_timer()


@contextmanager
def open_db_connection(connection_string, commit=False):
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    try:
        yield cursor
    except pyodbc.DatabaseError as err:
        error, = err.args
        sys.stderr.write(error.message)
        cursor.execute("ROLLBACK")
        raise err
    else:
        if commit:
            cursor.execute("COMMIT")
        else:
            cursor.execute("ROLLBACK")
    finally:
        connection.close()


def write_to_csv(data, filename):
    with open(filename, 'w', newline='\n') as file:
        writer = csv.writer(file, delimiter='\t')
        if data:  # check if data is not empty

            if os.stat(filename).st_size == 0:
                # write the headers
                print('writing the header')
                writer.writerow(data[0].keys())
            # write the values
            for record in data:
                writer.writerow(record.values())
                # writer.writerow('\n')


def save_data(table, file_name, table_name):
    # removed 11/29/2023 to see if performance is increased
    folder = r'\\accounts.wistate.us\etf\files\prod\Support_Svcs\IT\BI\Data_Sharing-R\Data Extracts\DEV\IAS_Conversion'
    write_to_csv(table, f'{folder}\\{file_name}')
    if os.path.getsize(f'{folder}\\{file_name}') > 0:  # Check if file is not empty
        print('hi file not empty')
        # regular_insert(f'ias_recon.{table_name}', file_name, server)
        # bulk_insert(f'ias_recon.{table_name}', f'{folder}/{file_name}', server)


def bulk_insert(table_name, file_path, server_name):
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        fr'SERVER={server_name};'
        r'DATABASE=ETF_DL_REFINED;'
        r'Trusted_Connection=yes;'
    )
    sql_string = f"BULK INSERT {table_name} FROM '{file_path}' WITH (FORMAT = 'CSV');"
    print(sql_string)
    with contextlib.closing(pyodbc.connect(conn_str)) as conn:
        with contextlib.closing(conn.cursor()) as cursor:
            cursor.execute(sql_string)
        conn.commit()


def regular_insert(table_name, filepath, server_name):
    conn_str = (
        r'DRIVER={ODBC Driver 17 for SQL Server};'
        fr'SERVER={server_name};'
        r'DATABASE=ETF_DL_REFINED;'
        r'Trusted_Connection=yes;'
    )
    with open_db_connection(conn_str) as cursor:

        with open(filepath, 'r', newline='') as file:
            reader = csv.reader(file, delimiter='\t')
            try:
                columns = next(reader)  # Assuming the first row contains column names
            except StopIteration:
                print(f"No data found in file {filepath}")
                return

            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?' for col in columns])})"

            for row in reader:
                cursor.execute(query, row)
            cursor.commit()


def safe_find(element, tag):
    result = element.find(tag)
    return result.text if result is not None else None


def process_data(contracts_table, members_table, addresses_table, phone_numbers_table, emails_table, categories_table,
                 medicare_table, benefits_table, financial_contributions_table, financial_benefit_details_table,
                 addl_insurance_table):
    if contracts_table:
        save_data(contracts_table, 'Contracts.csv', 'Contracts')
    if members_table:
        save_data(members_table, 'Members.csv', 'Members')
    if addresses_table:
        save_data(addresses_table, 'Addresses.csv', 'Addresses')
    if phone_numbers_table:
        save_data(phone_numbers_table, 'PhoneNumbers.csv', 'PhoneNumbers')
    if emails_table:
        save_data(emails_table, 'Emails.csv', 'Emails')
    if categories_table:
        save_data(categories_table, 'Categories.csv', 'Categories')
    if medicare_table:
        save_data(medicare_table, 'Medicare.csv', 'Medicare')
    if benefits_table:
        save_data(benefits_table, 'Benefit.csv', 'Benefit')
    if financial_contributions_table:
        save_data(financial_contributions_table, 'FinancialContributions.csv', 'FinancialContributions')
    if financial_benefit_details_table:
        save_data(financial_benefit_details_table, 'FinancialBenefitDetails.csv', 'FinancialBenefitDetails')
    if addl_insurance_table:
        save_data(addl_insurance_table, 'AdditionalInsurances.csv', 'AdditionalInsurances')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please provide the path to the XML file and server name as arguments.")
        sys.exit(1)
    sponsor_count = 0
    contract_count = 0
    member_count = 0
    benefit_count = 0
    file_path = sys.argv[1]
    server = sys.argv[2]
    context_file_metadata = ET.iterparse(file_path, events=('end',), tag='FileMetaData')
    context_sender = ET.iterparse(file_path, events=('end',), tag='Sender')
    context_sponsor = ET.iterparse(file_path, events=('end',), tag='Sponsor')

    # Tables
    file_meta_data_table = []
    sender_table = []
    contracts_table = []
    members_table = []
    addresses_table = []
    phone_numbers_table = []
    emails_table = []
    categories_table = []
    benefits_table = []
    financial_contributions_table = []
    financial_benefit_details_table = []
    addl_insurance_table = []
    medicare_table = []

    filename = None
    sender_taxID = None

    # FileMetaData Table
    for _, file_meta_data in context_file_metadata:
        file_meta_data_record = {
            "FileName": safe_find(file_meta_data, 'FileName'),
            "FileType": safe_find(file_meta_data, 'FileType'),
            "FileID": safe_find(file_meta_data, 'FileID'),
            "SponsorCount": safe_find(file_meta_data, 'SponsorCount'),
            "ContractCount": safe_find(file_meta_data, 'ContractCount'),
            "SenderID": safe_find(file_meta_data, 'SenderID'),
            "SentDate": safe_find(file_meta_data, 'SentDate'),
            "SentTime": safe_find(file_meta_data, 'SentTime'),
            "ReceiverID": safe_find(file_meta_data, 'ReceiverID'),
            "SponsoringCarrierID": safe_find(file_meta_data, 'SponsoringCarrierID'),
            "UsageInd": safe_find(file_meta_data, 'UsageInd')
        }
        file_meta_data_table.append(file_meta_data_record)
        filename = safe_find(file_meta_data, 'FileName')
    # Sender Table
    for _, sender in context_sender:
        sender_record = {
            "Name": sender.find('Name').text,
            "TaxID": sender.find('TaxID').text,
            "InsurerName": sender.find('InsurerName').text,
            "InsurerID": sender.find('InsurerID').text,
            # Linking to FileMetaData via FileID
            "RK_FileMetaData_FileName": filename
        }
        sender_table.append(sender_record)
        sender_taxID = sender.find('TaxID').text
    # test
    # save_data(file_meta_data_table, 'FileMetaData.csv', 'FileMetaData')
    # save_data(sender_table, 'Sender.csv', 'Sender')

    # Write data to CSV and bulk insert for FileMetaData and Sender
    # [same code as provided]
    # this is new test
    for _, sponsor in context_sponsor:
        sponsor_count += 1
        sponsor_GroupIdentifier = None
        sponsors_table = []
        sponsor_record = {
            "Sponsor_ID": sponsor_count,
            "Name": sponsor.find('Name').text,
            "GroupIdentifier": sponsor.find('GroupIdentifier').text,
            # Linking to Sender via TaxID
            "RK_Sender_TaxID": sender_taxID,
            "RK_FileMetaData_FileName": filename
        }
        sponsors_table.append(sponsor_record)
        sponsor_GroupIdentifier = sponsor.find('GroupIdentifier').text
        # save_data(sponsors_table, 'Sponsors.csv', 'Sponsors')

        for contract in sponsor.iter('Contract'):
            contract_SubscriberID = None
            # contracts_table = []
            # members_table = []
            # addresses_table = []
            # phone_numbers_table = []
            # emails_table = []
            # categories_table = []
            # benefits_table = []
            # financial_contributions_table = []
            # financial_benefit_details_table = []
            # addl_insurance_table = []
            # medicare_table = []
            contract_count += 1
            contract_record = {
                "Sponsor_ID": sponsor_count,
                "Contract_ID": contract_count,
                "SubscriberID": safe_find(contract, 'SubscriberID'),
                "TransactionType": contract.find('Metadata/TransactionType').text,
                # Linking to Sponsor via GroupIdentifier
                "RK_Sponsor_GroupIdentifier": sponsor_GroupIdentifier,
                "RK_FileMetaData_FileName": filename,
            }
            contracts_table.append(contract_record)
            contract_SubscriberID = safe_find(contract, 'SubscriberID')
            contract_count += 1
            for member in contract.iter('Member'):
                member_count += 1
                member_UPID = None
                member_record = {
                    "Contract_ID": contract_count,
                    "Member_ID": member_count,
                    "FirstName": safe_find(member, 'FirstName'),
                    "LastName": safe_find(member, 'LastName'),
                    "Relationship": safe_find(member, 'Relationship'),
                    "PayrollID": safe_find(member, 'PayrollID'),
                    "UPID": safe_find(member, 'UPID'),
                    "SSOID": safe_find(member, 'SSOID'),
                    "SSN": safe_find(member, 'SSN'),
                    "Gender": safe_find(member, 'Gender'),
                    "PersonType": safe_find(member, 'PersonType'),
                    "BirthDate": safe_find(member, 'BirthDate'),
                    "MaritalStatus": safe_find(member, 'MaritalStatus'),
                    "Ethnicity": safe_find(member, 'Ethnicity'),
                    "EnhancedEthnicity": safe_find(member, 'EnhancedEthnicity'),
                    "EnhancedRace": safe_find(member, 'EnhancedRace'),
                    "HandicapIndicator": safe_find(member, 'HandicapIndicator'),
                    "EarningsAmount": safe_find(member, 'EarningsAmount'),
                    "EarningsClass": safe_find(member, 'EarningsClass'),
                    "EarningsEffectiveDate": safe_find(member, 'EarningsEffectiveDate'),
                    "PayPeriod": safe_find(member, 'PayPeriod'),
                    "AdvancedEarningsAmount": safe_find(member, 'AdvancedEarningsAmount'),
                    "AdvancedEarningsClass": safe_find(member, 'AdvancedEarningsClass'),
                    "AdvancedEarningsEffectiveDate": safe_find(member, 'AdvancedEarningsEffectiveDate'),
                    "WorkState": safe_find(member, 'WorkState'),
                    "HireDate": safe_find(member, 'HireDate'),
                    "AdjustedServiceDate": safe_find(member, 'AdjustedServiceDate'),
                    "TermDate": safe_find(member, 'TermDate'),
                    "TermReason": safe_find(member, 'TermReason'),
                    "RK_Contract_SubscriberID": contract.find('SubscriberID').text,
                    "RK_FileMetaData_FileName": filename,
                }
                members_table.append(member_record)
                member_UPID = safe_find(member, 'UPID')

                # Address
                address = member.find('Address')
                if address is not None:
                    address_record = {
                        "Member_ID": member_count,
                        "PrimaryStreet": safe_find(address, 'PrimaryStreet'),
                        "SecondaryStreet": safe_find(address, 'SecondaryStreet'),
                        "City": safe_find(address, 'City'),
                        "State": safe_find(address, 'State'),
                        "PostalCode": safe_find(address, 'PostalCode'),
                        "CountryCode": safe_find(address, 'CountryCode'),
                        "AddressType": "PhysicalAddress",
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    addresses_table.append(address_record)

                # AlternateAddresses
                for alt_address in member.findall('AlternateAddresses/MailingAddress'):
                    alt_address_record = {
                        "Member_ID": member_count,
                        "PrimaryStreet": safe_find(alt_address, 'PrimaryStreet'),
                        "SecondaryStreet": safe_find(alt_address, 'SecondaryStreet'),
                        "City": safe_find(alt_address, 'City'),
                        "State": safe_find(alt_address, 'State'),
                        "PostalCode": safe_find(alt_address, 'PostalCode'),
                        "CountryCode": safe_find(alt_address, 'CountryCode'),
                        "AddressType": "MailingAddress",
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    addresses_table.append(alt_address_record)
                for alt_address in member.findall('AlternateAddresses/BillingAddress'):
                    alt_address_record = {
                        "Member_ID": member_count,
                        "PrimaryStreet": safe_find(alt_address, 'PrimaryStreet'),
                        "SecondaryStreet": safe_find(alt_address, 'SecondaryStreet'),
                        "City": safe_find(alt_address, 'City'),
                        "State": safe_find(alt_address, 'State'),
                        "PostalCode": safe_find(alt_address, 'PostalCode'),
                        "CountryCode": safe_find(alt_address, 'CountryCode'),
                        "AddressType": "MailingAddress",
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    addresses_table.append(alt_address_record)

                # Phone Numbers
                for phone in member.findall('PhoneNumbers/PhoneNumber'):
                    phone_record = {
                        "Member_ID": member_count,
                        "Number": phone.text,
                        "Type": phone.get('type'),
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    phone_numbers_table.append(phone_record)

                # Assuming there's an Email tag in your XML structure
                for email in member.findall('EmailAddresses/EmailAddress'):
                    email_record = {
                        "Member_ID": member_count,
                        "Email": email.text,
                        "Type": email.get('type'),
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    emails_table.append(email_record)

                # Categories
                for category in member.findall('Categories/Category'):
                    category_record = {
                        "Member_ID": member_count,
                        "Value": safe_find(category, 'Value'),
                        "EffectiveDate": safe_find(category, 'EffectiveDate'),
                        "Name": safe_find(category, 'Name'),
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    categories_table.append(category_record)

                # Medicare Table
                medicare = member.find('Medicare')
                if medicare is not None:
                    medicare_record = {
                        "Member_ID": member_count,
                        "HICNumber": safe_find(medicare, 'HICNumber'),
                        "EffectiveDate": safe_find(medicare, 'EffectiveDate'),
                        "EndDate": safe_find(medicare, 'EndDate'),
                        "EligibilityReason": safe_find(medicare, 'EligibilityReason'),
                        "EligibilityDate": safe_find(medicare, 'EligibilityDate'),
                        "MedicareType": safe_find(medicare, 'MedicareType'),
                        "RK_Member_UPID": member_UPID,
                        "RK_FileMetaData_FileName": filename,
                    }
                    medicare_table.append(medicare_record)
                # Benefits Table
                for benefit in member.findall('Benefits/Benefit'):
                    benefit_count += 1
                    benefit_record = {
                        "Benefit_ID": benefit_count,
                        "Member_ID": member_count,
                        "BenefitType": benefit.get('BenefitType'),
                        "TransactionType": safe_find(benefit, 'TransactionType'),
                        "CoverageIndicator": safe_find(benefit, 'CoverageIndicator'),
                        "ProductID": safe_find(benefit, 'ProductID'),
                        "CoverageEffectiveDate": safe_find(benefit, 'CoverageEffectiveDate'),
                        "SalaryMultiplier": safe_find(benefit, 'SalaryMultiplier'),
                        "CoverageAmount": safe_find(benefit, 'CoverageAmount'),
                        "RK_Member_UPID": member_UPID,
                        "RK_Contract_SubscriberID": contract.find('SubscriberID').text,
                        "RK_FileMetaData_FileName": filename,
                    }
                    benefits_table.append(benefit_record)

                    # FinancialContributions Table
                    for financial_contribution in benefit.findall('FinancialContributions/FinancialContribution'):
                        financial_contribution_record = {
                            "Benefit_ID": benefit_count,
                            "ContributionType": safe_find(financial_contribution, 'ContributionType'),
                            "StartDate": safe_find(financial_contribution, 'StartDate'),
                            "EndDate": safe_find(financial_contribution, 'EndDate'),
                            "ContributionAmount": safe_find(financial_contribution, 'ContributionAmount'),
                            "RK_Benefit_ProductID": benefit.find('ProductID').text,
                            "RK_Member_UPID": member_UPID,
                            "RK_FileMetaData_FileName": filename,
                        }
                        financial_contributions_table.append(financial_contribution_record)

                    # FinancialBenefitDetails Table
                    financial_benefit_detail = benefit.find('FinancialBenefitDetail')
                    if financial_benefit_detail:
                        financial_benefit_detail_record = {
                            "Benefit_ID": benefit_count,
                            "TotalAnnualElection": safe_find(financial_benefit_detail, 'TotalAnnualElection'),
                            "MemberAnnualElection": safe_find(financial_benefit_detail, 'MemberAnnualElection'),
                            "RK_Benefit_ProductID": benefit.find('ProductID').text,
                            "RK_Member_UPID": member_UPID,
                            "RK_FileMetaData_FileName": filename,
                        }
                        financial_benefit_details_table.append(financial_benefit_detail_record)
                    # addl_insurance Table
                    for insurance in member.findall('AdditionalInsurances/AdditionalInsurance'):
                        insurance_record = {
                            "Member_ID": member_count,
                            "InsuranceType": safe_find(insurance, 'InsuranceType'),
                            "TransactionType": safe_find(insurance, 'TransactionType'),
                            "CoverageIndicator": safe_find(insurance, 'CoverageIndicator'),
                            "ProductID": safe_find(insurance, 'ProductID'),
                            "CoverageEffectiveDate": safe_find(insurance, 'CoverageEffectiveDate'),
                            "CoverageAmount": safe_find(insurance, 'CoverageAmount'),
                            "RK_Member_UPID": member_UPID,
                            "RK_FileMetaData_FileName": filename,
                        }
                        addl_insurance_table.append(insurance_record)
                member.clear()

            # threads = []
            # t = threading.Thread(target=process_data, args=(
            #     contracts_table, members_table, addresses_table, phone_numbers_table, emails_table, categories_table,
            #     medicare_table, benefits_table, financial_contributions_table, financial_benefit_details_table,
            #     addl_insurance_table))
            # threads.append(t)
            # t.start()
            # for t in threads:
            #     t.join()
            # Clear the processed sponsor to free memory
            contract.clear()
            # member.clear()

            # Also clear out any elements above the Sponsor in the XML tree
            while contract.getprevious() is not None:
                del contract.getparent()[0]

        # Clear the processed sponsor to free memory
        sponsor.clear()

        # Also clear out any elements above the Sponsor in the XML tree
        while sponsor.getprevious() is not None:
            del sponsor.getparent()[0]

    threads = []
    t = threading.Thread(target=process_data, args=(
        contracts_table, members_table, addresses_table, phone_numbers_table, emails_table, categories_table,
        medicare_table, benefits_table, financial_contributions_table, financial_benefit_details_table,
        addl_insurance_table))
    threads.append(t)
    t.start()
    for t in threads:
        t.join()

    toc = timeit.default_timer()
    tictoc = toc = timeit.default_timer()
    print(tictoc)
