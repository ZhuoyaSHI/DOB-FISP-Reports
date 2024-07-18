import re
from bs4 import BeautifulSoup, NavigableString
import html
from lxml import etree
"""
Filename: report_process.py

Description:
This script generate FISP report text through filling the original html with related fields in response content.

Date: 2024/07/07

Functions:
- getText(htmlFile,content): Processes an HTML file and populates placeholders with data from the provided content.

"""

def getText(htmlFile,content):
    """ Args:
            htmlFile: Path to the HTML file.
            content: Response content containing data for field replacements.

        Returns:
            str: The processed HTML report as plain text.
    """
    
    # Extract FISP fields using XPath
    FISPhtml = etree.parse("FISPReports.html",etree.HTMLParser(encoding='utf-8'))
    Nodes = FISPhtml.xpath('//*[contains(text(), "ngDialogData")]')
    pattern = r'ngDialogData(?:\.\w+)+'
    FISP_fields = []
    
    for node in Nodes:
        text_content = node.xpath('string()')
        matches = re.findall(pattern, text_content)
        FISP_fields.extend([match.replace('ngDialogData.', '') for match in matches])
    
    # Read the HTML content and create a BeautifulSoup object
    with open(htmlFile, 'r', encoding='utf-8') as file:
        FISPhtml = file.read()
    soup = BeautifulSoup(FISPhtml, 'lxml')

    # Replace placeholders using FISP fields and response content
    for field in FISP_fields:
        keys = field.split('.')
        value = content.get(keys[0])  # Get value from top-level key in content
        if value is not None:
            for i in keys[1:]:
                value = value.get(i)  # Traverse nested dictionaries
        value = html.escape(str(value)) if value is not None else ''  # Escape HTML characters
        placeholder = soup.find(string=lambda text: f'{{{{ngDialogData.{field}' in str(text))
        if placeholder:  # Replace placeholder with extracted value
            new_tag = soup.new_string(value)
            placeholder.replace_with(new_tag)

    # Process "Exterior Wall" tables
    Exterior_Wall_Type_table = soup.select_one("body > table > tbody > tr:nth-child(2) > td:nth-child(1) > table")
    Exterior_Wall_Material_table = soup.select_one("body > table > tbody > tr:nth-child(2) > td:nth-child(2) > table")

    SelectedExteriorWallType = content.get("locationDetails").get("SelectedExteriorWallType","")
    SelectedExteriorWallMaterial = content.get("locationDetails").get("SelectedExteriorWallMaterial","")

    tbody1 = Exterior_Wall_Type_table.find("tbody")
    tbody2 = Exterior_Wall_Material_table.find("tbody")

    # Clear existing rows in the tables
    for tr in tbody1.find_all("tr"):
        tr.extract()

    # Create and populate new rows for "Exterior Wall" tables
    for item in SelectedExteriorWallType:
        tr = soup.new_tag("tr")
        td1 = soup.new_tag("td")
        td1.string = item.get("WallType") if item.get("WallType") else ""
        tr.append(td1)
        tbody1.append(tr)

    # Repeat the workflow
    for tr in tbody2.find_all("tr"):
        tr.extract()

    for item in SelectedExteriorWallMaterial:
        tr = soup.new_tag("tr")
        td1 = soup.new_tag("td")
        td1.string = item.get("WallMaterial") if item.get("WallMaterial") else ""
        tr.append(td1)
        tbody2.append(tr)

    # Process "comments" table
    comment_table = soup.select_one("body > div.col-xs-12.col-sm-12.col-md-12.pad-none > div > div > table")
    table = comment_table.find('tbody')

    # Get the list of inspections from the content dictionary
    comments = content.get("InspectionsDates","")

    # Preserve the table header row
    table_header = table.find('tr') 

    # Clear the existing table body content, leaving only the header row
    table.clear()
    table.append(table_header)

    # If there are comments, add new rows to the table
    if comments != "":
        for inspection in comments:
            new_row = soup.new_tag('tr')

            # Create a table cell for the inspection date
            date_cell = soup.new_tag('td')
            date_text = inspection.get('InspectionDate', '')
            date_cell.string = date_text if date_text else ''
            new_row.append(date_cell)
            
            # Create a table cell for the inspection comments
            comments_cell = soup.new_tag('td')
            comments_text  = inspection.get('Comments', '')
            comments_cell.string = comments_text if comments_text else ''
            new_row.append(comments_cell)

            # Add the new row to the table
            table.append(new_row)
        
    # Clean up special characters in the HTML
    for tag in soup.find_all():
        if tag.string:
            tag.string = html.unescape(tag.string)

    # Remove all style attributes from HTML tags
    for tag in soup.find_all(style=True):
        del tag['style']

    # Extract the contents of nested <span> tags
    for span in soup.find_all('span'):
        span.replace_with(span.get_text())

    # Convert HTML to plain text
    for script in soup(["script", "style"]):
        script.decompose()

    # Get the text content of the modified HTML
    FISPtext = soup.get_text(separator='\n')

    # Remove remaining HTML tags using regular expressions
    FISPtext = re.sub(r'<[^>]+>', '', FISPtext)
    FISPtext = html.unescape(FISPtext)

    # Return the processed plain text
    return (FISPtext)