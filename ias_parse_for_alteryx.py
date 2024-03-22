from lxml import etree as ET
import contextlib
from contextlib import contextmanager
import sys
import csv
import pyodbc
import os
import random
import timeit
import itertools

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


def save_data(table, file_name):
    # removed 11/29/2023 to see if performance is increased
    # folder = r'\\accounts.wistate.us\etf\files\prod\Support_Svcs\IT\BI\Data_Sharing-R\Data Extracts\DEV\IAS_Conversion'
    write_to_csv(table, f'{folder_name}\\{file_name}')


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


def create_file_metadata(file_path):
    context_file_metadata = ET.iterparse(file_path, events=('end',), tag='FileMetaData')
    # FileMetaData Table
    file_meta_data_table = []
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
        # save_data(file_meta_data_table, 'FileMetaData.csv', 'FileMetaData')


def create_sender_table(file_path):
    sender_table = []
    context_sender = ET.iterparse(file_path, events=('end',), tag='Sender')
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
    # save_data(sender_table, 'Sender.csv', 'Sender')


# this is the function that grabs the category value based on the category name
def get_cat_value(cat_name, categories):
    cat_value = [category.find('Value').text for category in categories
                 if category.find('Name').text == cat_name]
    return cat_value[0] if len(cat_value) > 0 else ''


def get_medicare_value(m, value):
    medicare = m.find('Medicare')
    if medicare is not None:
        return safe_find(medicare, value)
    else:
        return ''


def shuffle_string(string):
    if string is not None:
        l = list(string)
        random.shuffle(l)
        return ''.join(l)
    else:
        return ''


def check_for_item(item, field):
    if item is not None:
        return safe_find(item, field)
    else:
        return ''


def get_insurance(d):
    is_d_none = d is not None
    insurance_record = {
        "AdditionalInsurance_AdditionalInsuranceType": check_for_item(d, 'AdditionalInsuranceType'),
        "AdditionalInsurance_Carrier": check_for_item(d, 'Carrier'),
        "AdditionalInsurance_EffectiveDate": check_for_item(d, 'EffectiveDate'),
        "AdditionalInsurance_EndDate": check_for_item(d, 'EndDate'),
        "AdditionalInsurance_BenefitType": check_for_item(d, 'BenefitType'),
        "AdditionalInsurance_PolicyHolderDOB": check_for_item(d, 'PolicyHolderDOB'),
        "AdditionalInsurance_PolicyHolderName": check_for_item(d, 'PolicyHolderName'),
        "AdditionalInsurance_PolicyHolderRelationship": check_for_item(d, 'PolicyHolderRelationship'),
        "AdditionalInsurance_PolicyHolderSSN": check_for_item(d, 'PolicyHolderSSN'),
        "AdditionalInsurance_PolicyNumber": check_for_item(d, 'PolicyNumber'),
        "AdditionalInsurance_PrimaryInsured": check_for_item(d, 'PrimaryInsured')
    }
    return insurance_record


def get_email(c):
    is_c_none = c is not None
    email = {
        "Email_EmailAddress": c.text if is_c_none else '',
        "Email_Email_Type_CD": c.get('type') if is_c_none else ''
    }
    return email


def get_phone_number(b):
    is_b_none = b is not None
    phone_record = {
        "Phone_PhoneNumber": b.text if is_b_none else '',
        "Phone_Phone_Type_CD": b.get('type') if is_b_none else ''
    }
    return phone_record


def get_address(a):
    address_record = {
        "Address_PrimaryStreet": check_for_item(a, 'PrimaryStreet'),
        "Address_SecondaryStreet": check_for_item(a, 'SecondaryStree'),
        "Address_City": check_for_item(a, 'City'),
        "Address_State": check_for_item(a, 'State'),
        "Address_PostalCode": check_for_item(a, 'PostalCode'),
        "Address_CountryCode": check_for_item(a, 'CountryCode'),
        "Address_AddressType_CD": check_for_item(a, 'AddressType_CD')}
    return address_record


def merge_demo_records(addresses, phones, emails, base_record, insurances):
    for (a, b, c, d) in itertools.zip_longest(addresses, phones, emails, insurances):
        final_row = base_record | get_address(a) | get_phone_number(b) | get_email(c) | get_insurance(d)
        # print(final_row)
        demo_records.append(final_row)


def get_benefit(benefit):
    benefit_record = {
        "BenefitType": benefit.get('BenefitType'),
        "TransactionType": safe_find(benefit, 'TransactionType'),
        "CoverageIndicator": safe_find(benefit, 'CoverageIndicator'),
        "ProductID": safe_find(benefit, 'ProductID'),
        "CoverageEffectiveDate": safe_find(benefit, 'CoverageEffectiveDate'),
        "CoverageEndDate": safe_find(benefit, 'CoverageEndDate'),
        "SalaryMultiplier": safe_find(benefit, 'SalaryMultiplier'),
        "CoverageAmount": safe_find(benefit, 'CoverageAmount'),
    }
    return benefit_record


def get_fc(fc):  # fc stands for financial contribution
    financial_contribution_record = {
        "FinancialContribution_ContributionType": check_for_item(fc, 'ContributionType'),
        "FinancialContribution_StartDate": check_for_item(fc, 'StartDate'),
        "FinancialContribution_EndDate": check_for_item(fc, 'EndDate'),
        "FinancialContribution_ContributionAmount": check_for_item(fc, 'ContributionAmount'),
    }
    return financial_contribution_record


def get_fbd(fbd):  # stands for financial benefit detail
    financial_benefit_detail_record = {
        "FinancialBenefitDetail_TotalAnnualElection": check_for_item(fbd, 'TotalAnnualElection')
    }
    return financial_benefit_detail_record


def create_benefits(benefits, etf_member_id, employer_number, person_type, subscriber_id):
    for benefit in benefits:
        financial_contributions = benefit.findall('FinancialContributions/FinancialContribution')
        financial_benefit_details = benefit.findall('FinancialBenefitDetail')
        benefit_base_record = {
            "ETF_Member_ID": etf_member_id,
            "Member_PersonType": person_type,
            "Subscriber_SSN": subscriber_id,
            "Employer_Number": employer_number
        }
        for (a, b, c) in itertools.zip_longest(benefit, financial_contributions, financial_benefit_details):
            final_row = benefit_base_record | get_benefit(benefit) | get_fc(b) | get_fbd(c)
            print(final_row)
            benefit_records.append(final_row)


def get_imax_file_name_and_date(file_name):
    cnxn = pyodbc.connect("DSN=ETF_DL_REFINED")
    if file_name is None or len(file_name) == 0:
        sql = ("SELECT  [FileName],[DW_Insert_Timestamp] FROM [ias_conv].[FileMetaData] where [FileMetaData_ID] = ( "
               "select max([FileMetaData_ID]) from [ias_conv].[FileMetaData])")
    else:
        sql = f"SELECT  TOP 1 [FileName],[DW_Insert_Timestamp] FROM [ias_conv].[FileMetaData] where [FileName] = '{file_name}'"
    # print(sql)
    cursor = cnxn.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        new_file_name = row[0]
        time_stamp = row[1]
        return new_file_name, time_stamp
    return '', None


def create_row(sponsor, contract, member):
    addresses = []
    address = member.find('Address')
    mailing_address = member.find('AlternateAddresses/MailingAddress')
    billing_address = member.find('AlternateAddresses/BillingAddress')
    if address is not None:
        addresses.append(address)
    if mailing_address is not None:
        addresses.append(mailing_address)
    if billing_address is not None:
        addresses.append(billing_address)
    phone_numbers = member.findall('PhoneNumbers/PhoneNumber')
    emails = member.findall('EmailAddresses/EmailAddress')
    insurances = member.findall('AdditionalInsurances/AdditionalInsurance')
    categories = member.findall('Categories/Category')
    category_effective_date = categories[0].find("EffectiveDate").text if len(categories) > 0 else ''
    # print(category_effective_date)
    subscriber_id = safe_find(contract, 'SubscriberID')
    employer_number = sponsor.find('GroupIdentifier').text
    etf_member_id = safe_find(member, 'UPID')
    person_type = safe_find(member, 'PersonType')
    base_record = {
        "File_Date": file_date,
        "Sponsor_GroupIdentifier": employer_number,
        "Sponsor_Name": sponsor.find('Name').text,
        "Contract_SubscriberID": subscriber_id,
        "Employer": employer_number,
        "Employer_Number": employer_number,
        "Member_BirthDate": safe_find(member, 'BirthDate'),
        "Member_DeceasedDate": safe_find(member, 'DeceasedDate'),
        "Member_FirstName": safe_find(member, 'FirstName'),
        "Member_MiddleName": safe_find(member, 'MiddleName'),
        "Member_LastName": safe_find(member, 'LastName'),
        "Member_Gender": safe_find(member, 'Gender'),
        "Member_Relationship": safe_find(member, 'Relationship'),
        "Member_PayrollID": safe_find(member, 'PayrollID'),
        "Member_UPID": etf_member_id,
        "Member_Suffix": safe_find(member, 'Suffix'),
        "Member_SSN": safe_find(member, 'SSN'),
        "Member_PersonType": person_type,
        "Member_MaritalStatus": safe_find(member, 'MaritalStatus'),
        "Member_EffectiveChangeDate": safe_find(member, "EffectiveChangeDate"),
        "Member_Ethnicity": safe_find(member, 'Ethnicity'),
        "Member_EnhancedEthnicity": safe_find(member, 'EnhancedEthnicity'),
        "Member_EnhancedRace": safe_find(member, 'EnhancedRace'),
        "Member_HandicapIndicator": safe_find(member, 'HandicapIndicator'),
        "MemberEmployment_EarningsAmount": safe_find(member, 'EarningsAmount'),
        "MemberEmployment_EarningsEffectiveDate": safe_find(member, 'EarningsEffectiveDate'),
        "MemberEmployment_AdvancedEarningsAmount": safe_find(member, 'AdvancedEarningsAmount'),
        "MemberEmployment_AdvancedEarningsEffectiveDate": safe_find(member, 'AdvancedEarningsEffectiveDate'),
        "MemberEmployment_AdjustedServiceDate": safe_find(member, 'AdjustedServiceDate'),
        "MemberEmployment_PayPeriod": safe_find(member, 'PayPeriod'),
        "MemberEmployment_EarningsClass": safe_find(member, 'EarningsClass'),
        "MemberEmployment_AdvancedEarningsClass": safe_find(member, 'AdvancedEarningsClass'),
        "MemberEmployment_HireDate": safe_find(member, 'HireDate'),
        "MemberEmployment_TermDate": safe_find(member, 'TermDate'),
        "Life Premium Waiver": get_cat_value("Life Premium Waiver", categories),
        "Dual Employment": get_cat_value("Dual Employment", categories),
        "Vision Payment Source": get_cat_value("Vision Payment Source", categories),
        "ICI Premium Waiver": get_cat_value("ICI Premium Waiver", categories),
        "Tax Status": get_cat_value("Tax Status", categories),
        "Unique Plan Eligibility": get_cat_value("Unique Plan Eligibility", categories),
        "Life Payment Source": get_cat_value("Life Payment Source", categories),
        "Employee Type": get_cat_value("Employee Type", categories),
        "Out of State Employee": get_cat_value("Out of State Employee", categories),
        "Employer_Unit_Number": get_cat_value("Employer Unit", categories),
        "Health Payment Source": get_cat_value("Health Payment Source", categories),
        "ICI Contrib Wait Period Met": get_cat_value("ICI Contrib Wait Period Met", categories),
        "Legacy Life": get_cat_value("Legacy Life", categories),
        "Calendar Set": get_cat_value("Calendar Set", categories),
        "Dental Payment Source": get_cat_value("Dental Payment Source", categories),
        "Employer_Sub_Unit_Number": get_cat_value("Employer Sub-Unit", categories),
        "Employer Unit Program Option": get_cat_value("Employer Unit Program Option", categories),
        "Employment Status": get_cat_value("Employment Status", categories),
        "Employer Medical Surcharge": get_cat_value("Employer Medical Surcharge", categories),
        "Primary Employer": get_cat_value("Primary Employer", categories),
        "Under 70 When Hired": get_cat_value("Under 70 When Hired", categories),
        "ICI Premium Category": get_cat_value("ICI Premium Category", categories),
        "Medical Contrib Wait Period": get_cat_value("Medical Contrib Wait Period", categories),
        "Opt Out Incentive Eligible": get_cat_value("Opt Out Incentive Eligible", categories),
        "WRS Eligible": get_cat_value("WRS Eligible", categories),
        "Medical Premium Contribution": get_cat_value("Medical Premium Contribution", categories),
        "Protective Status": get_cat_value("Protective Status", categories),
        "CategoryEffectiveDate": category_effective_date,
        "Medicare_HICNumber": get_medicare_value(member, 'HICNumber'),
        "Medicare_EffectiveDate": get_medicare_value(member, 'EffectiveDate'),
        "Medicare_EndDate": get_medicare_value(member, 'EndDate'),
        "Medicare_EligibilityReason": get_medicare_value(member, 'EligibilityReason'),
        "Medicare_EligibilityDate": get_medicare_value(member, 'EligibilityDate'),
        "Medicare_MedicareType": get_medicare_value(member, 'MedicareType'),

    }
    merge_demo_records(addresses, phone_numbers, emails, base_record, insurances)
    benefits = member.findall('Benefits/Benefit')
    create_benefits(benefits, etf_member_id, employer_number, person_type, subscriber_id)


if __name__ == '__main__':
    # there are 3 possible parameters - db, folder_name, and file_name. In that order
    # file_name is NOT required, the other two are
    if len(sys.argv) < 3:
        print("Please provide the path to the XML file and server name as arguments.")
        sys.exit(1)
    server = sys.argv[1]
    folder_name = sys.argv[2]
    try:
        file_name = sys.argv[3]
    except IndexError:
        file_name = ''

    new_file_name, file_date = get_imax_file_name_and_date(file_name)
    if len(new_file_name) == 0 or file_date is None:
        print(
            f'Cannot find an entry in the database for {file_name}. Please make sure the file exist in ias_conv.FileMetaData table for this environment.')
        print(f'file_name: {file_name}')
        print(f'folder_name: {folder_name}')
    else:
        print(f'running the load for file {new_file_name} with file date {file_date}.')
    file_path = fr'{folder_name}\{new_file_name}'
    context_sponsor = ET.iterparse(file_path, events=('end',), tag='Sponsor')
    demo_records = []
    benefit_records = []
    # Tables
    filename = None
    # FileMetaData Table
    create_file_metadata(file_path)
    # Sender Table
    create_sender_table(file_path)

    # Write data to CSV and bulk insert for FileMetaData and Sender
    # [same code as provided]

    for _, sponsor in context_sponsor:
        for contract in sponsor.iter('Contract'):
            # contract_SubscriberID = None
            # contract_record = {
            #     "SubscriberID": safe_find(contract, 'SubscriberID'),
            #     "TransactionType": contract.find('Metadata/TransactionType').text,
            # }

            for member in contract.iter('Member'):
                # member_record = {
                # not sure where these come from
                #     "WorkState": safe_find(member, 'WorkState'),
                #     "TermReason": safe_find(member, 'TermReason'),
                # }
                create_row(sponsor, contract, member)
                member.clear()

            contract.clear()

            # Also clear out any elements above the Sponsor in the XML tree
            while contract.getprevious() is not None:
                del contract.getparent()[0]

        # Clear the processed sponsor to free memory
        sponsor.clear()

        # Also clear out any elements above the Sponsor in the XML tree
        while sponsor.getprevious() is not None:
            del sponsor.getparent()[0]

    save_data(demo_records, 'Demo_Records.csv')
    save_data(benefit_records, "Benefit_Records.csv")
    toc = timeit.default_timer()
    tictoc = toc = timeit.default_timer()
    print(tictoc)
