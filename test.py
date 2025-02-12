import requests
from bs4 import BeautifulSoup
import re
# URL of the published Google Doc



def decode_secret_message(url):
    # step 1 - Fetch the HTML content
    response = requests.get(url)

    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract and print text content
        text = soup.get_text()
        match = re.search(r'coordinate(.*)', text, re.DOTALL)
        print(text.strip())
        if match:
            data = match.group(1).strip()  # Get everything after "coordinate" and trim whitespace

            # Regular expression to find x, character, and y
            pattern = r'(\d+)([\u2580-\u259F])(\d)'
            matches = re.findall(pattern, data)


            for match in matches:
                x, char, y = match

            grid = {}
            max_x, max_y = 0, 0

            # Populate the grid with characters based on their coordinates
            for x, char, y in matches:
                x, y = int(x), int(y)
                grid[(x, y)] = char
                max_x = max(max_x, x)
                max_y = max(max_y, y)

            # Print rows from y=0 to max_y
            for y in range(max_y + 1):
                row = []
                for x in range(max_x + 1):
                    # for each value in the x row, fill with unicode or else append empty string
                    row.append(grid.get((x, y), ' '))
                print(''.join(row))  # Print the row as a single string


url = "https://docs.google.com/document/d/e/2PACX-1vQGUck9HIFCyezsrBSnmENk5ieJuYwpt7YHYEzeNJkIb9OSDdx-ov2nRNReKQyey-cwJOoEKUhLmN9z/pub"

decode_secret_message(url)