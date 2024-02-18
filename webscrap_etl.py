import polars as pl

def pages(pages:int)-> None:
    """This Function used to open given number of web pages sequentially

    Args:
        pages (int): number of pages need to open
    """
    
    # Import Library
    from selenium import webdriver
    
    # Instantiate module and its options to open the browser in backend
    options = webdriver.FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options = options )
    
    # Loop Through pages to run Web Scrapping each of it
    for i in range(pages):
        carts_on_page(driver,i+1)
        
    # Close the Browser
    driver.quit()
    
def carts_on_page(driver,page : int) -> None:
    """The Function used to run Web Scraping in the current page

    Args:
        driver (_type_): Driver that run browser and scrapping
        page (int): web page to scrap
    """
    # Import all Necessary libraries
    from models import KeyboardMechDL,ps_engine
    from sqlalchemy.orm import Session
    from bs4 import BeautifulSoup
    from selenium.webdriver.support import expected_conditions as ec
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from tqdm import tqdm
    import time
    
    # Set the Link with given page number
    pagination = 'https://www.tokopedia.com/search?navsource=home&page={page}&q=mechanical%20keyboard&source=universe&srp_component_id=02.02.02.01&st=product'
    
    # Open the web page
    driver.get(pagination.format(page=page))

    # Instantiate waiter to wait web page objects appear
    waiter = WebDriverWait(driver,60)
    
    # wait 2 secend
    time.sleep(2)
    
    # Wait the web page until specific objects class name appear
    carts = waiter.until(ec.visibility_of_all_elements_located((By.CLASS_NAME,'css-54k5sq')))
    
    # Count the number of objects are successfully captured then assign to len_carts variable
    len_carts = len(carts)

    # Declare variable contains list data type
    data = []
    
    # Loop Through number of objects in len_carts
    for i in tqdm(range(len_carts)):
        
        # Wait 2 seconds
        time.sleep(2)
        
        # Recapture the previous objects and assign to new_carts variable
        new_carts = waiter.until(ec.visibility_of_all_elements_located((By.CLASS_NAME,'css-54k5sq')))
        
        # Click the objects in sequence of i
        new_carts[i].click()
        
        # Wait 2 seconds 
        time.sleep(2)
        
        # Capture HTML structure of opened current webpage
        soup = BeautifulSoup(driver.page_source,'html.parser')
        
        # Try the steps and capture errors if any
        try:
            
            # Capture the title in web page
            title = soup.find(class_='css-1os9jjn').text
            
            # Capture the description in web page
            description = soup.find(class_='css-17zm3l eytdjj01').div.text
            
            # Assign to Keyboard Mech Data Lake Model and append to list in variable data
            data.append(
                KeyboardMechDL(title = title,description=description)
            )
        except Exception as e:
            # print the errors
            print(e)
        # Perform back in the browser
        driver.back()
    
    # Write the list of values in variable data to Data Lake in PostgreSQL
    with Session(ps_engine) as session:
        session.add_all(data)
        session.commit()



def extract() -> pl.LazyFrame:
    """The Functions used to check whether is Data Warehouse up to data or not\n
    and extract raw data from Data Lake and 

    Returns:
        polars.LazyFrame: Extracted Raw Data
    """
    
    from models import KeyboardMechDL,KeyBoardMechDWH,ps_engine,ps_uri
    from sqlalchemy import func
    
    
    # Get the name of table in KeyboardMechDL model
    dl = KeyboardMechDL.__tablename__
    
    # Try the followings step and capture errors if any
    try:
        # Connect the database then close it after the following steps completed
        with ps_engine.connect() as conn:
            # Get the latest id of keyboard_mech_dwh
            result = conn.execute(func.max(KeyBoardMechDWH.id)).first()[0]
            
            # if not exists the set the variable result to 0
            if not result:
                result = 0
            
        # Set the query that retrieve all data from keyboard_mech_dl that have id greater than the variable result
        query = f'select * from {dl} where id > {result}'
        
        # Retrieve the data and set it as LazyFrame
        lf = pl.read_database_uri(query,ps_uri).unique().lazy()
        print('Extract Step Complete!')
        return lf
    except Exception as e:
        # print error
        print(e)


def clean_transform(lf:pl.LazyFrame)-> pl.LazyFrame:
    """The function used to clean duplicated values and treat the missing values \n
    then transform the data to reliable format to Data Warehouse

    Args:
        lf (pl.LazyFrame): Extracted data

    Returns:
        pl.LazyFrame: Transformed and cleaned data
    """
    
    from utils import extract_number_keycaps, func_map, SHAPE_PROFILE,MATERIAL
    
    # Try the following steps and capture the errors if any
    try:
        # Check the description column is it having given material in the list of material and set the value
        material_lf = lf.with_columns(material = pl.col('description').map_elements(lambda x: func_map(x,MATERIAL)))
        
        # Check the description column is it having given shape profile in the list of shape profile set the value
        shape_profile_lf = material_lf.with_columns(
            shape_profile = pl.col('description')\
                                                .map_elements(lambda x: [i.lower() for i in SHAPE_PROFILE if i.lower() in x.lower() ]))
        
        # Check the description column is it having number before or after specific words and grab it
        key_lf = shape_profile_lf.with_columns(keys = pl.col('description')\
            .map_elements(extract_number_keycaps))
        
        # Normalize list of value in the given columns to row shape to ensure the columns has only 1 value and cast data type of keys column to integer
        normal_lf = key_lf.explode('material')\
                            .explode('shape_profile')\
                            .explode('keys')\
                            .cast({'keys':pl.Int32})

        # Select relevant columns that suited to Data Warehouse
        selection_lf = normal_lf.select(['id','title','material','shape_profile','keys'])
        
        # Drop rows that have all nulls in the given columns at the same rows 
        not_null_lf = selection_lf.filter(~pl.all_horizontal(pl.col("material").is_null() & pl.col("shape_profile").is_null() & pl.col('keys').is_null()))
        
        # Execute all steps above
        not_null_df = not_null_lf.collect()
        print('Clean and Transform Step Complete!')
        return not_null_df
    except Exception as e :
        # Print errors
        print('Data did not pass the validation or ')
        print(e)

def load(df: pl.DataFrame) -> None:
    """The function used load the trasformed and cleaned data to Data Warehouse

    Args:
        lf (pl.DataFrame): _description_
    """
    
    from models import KeyBoardMechDWH,ps_engine
    from sqlalchemy import insert
    
    try:
        # Convert DataFrame to list of dict
        df = df.to_dicts()
        
        # Connect to Data base
        with ps_engine.connect() as conn:
            # Write the value to Database
            conn.execute(insert(KeyBoardMechDWH),df)
            conn.commit()
        print('Load Step Complete!')
    except Exception as e :
        print(e)

if __name__ == '__main__':
    
    try:
        # Execute function pages to start Web Scraping
        # pages(5)
        print('The data is successfully extracted and loaded to DL')
    except Exception as e:
        print(e)
    
    # Execute ETL Functions
    load(clean_transform(extract()))
