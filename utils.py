
# list of common keycaps material in the ecommerce
MATERIAL = {
    ' abs ':'abs',
    'acrylonitrile butadiene styrene': 'abs',
    ' pbt ':'pbt',
    'Polybutylene Terephthalate':'pbt',
    ' pom ':'pom',
    'Polyoxymethylene':'pom',
    'Polycarbonate':'pc',
    'metal':'metal',
    'wood':'wood',
    'resin':'resin'
}


# list of common shape profile in the ecommerce
SHAPE_PROFILE = [' OEM ',
                 ' Cherry ',
                 ' SA ',
                 ' DSA ',
                 ' XDA ',
                 ' tai-hao ',
                 ' kat ',
                 ' dsa ',
                 ' kam ',
                 ' tea ',
                 ' mbk ',
                 
]


def func_map(value:str,map_val:dict)-> list:
    """The Function used to map thedict of value if the value in the list exists in the value

    Args:
        value (str): value to be map
        map_val (dict): the values to map

    Returns:
        list: mapped values
    """
    
    data = []
    
    # loop through ma_value 
    for key,val in map_val.items():
        # if key in map_value is in value append val to data
        if key.lower() in value.lower():
            data.append(val.lower())
    return data

def extract_number_keycaps(text:str):
    """The function used to find given pattern of words using regex and grab the value before or after

    Args:
        text (str): Value to check

    Returns:
        list | None: List of data or none
    """
    
    import re 
    # Some Patterns
    p1 = r"\b(?:number of keys:)\s+(\d+)"
    p2 = r'(\d+)\s+(?:key|tombol)\b'
    p3 = r'\b(?:key|tombol)\s+(\d+)'
    
    # List of pattern
    pattern = [p1,p2,p3]
    
    data = []
    
    # loop through list of pattern and grab the value
    for i in pattern:
        match = re.findall(i, text.lower())
        data += match
    return data if data else None

