from datetime import datetime



def standardize_date_format(raw_date: str) -> str:
    # Should be made with a unit test

    accepted_formats = [
        '%d %B %Y', # 1 January 2020
        '%d/%m/%Y', # 25/05/2020,
        '%Y-%m-%d'  # 2020-05-20
    ]

    for format in accepted_formats:
        try:
            return datetime.strptime(raw_date, format)
        except ValueError:
            pass 
    
    # If we reach this code it means we couldn t recognize the date, 
    # we probably need to add a new format

    raise ValueError(f'Could not cast {raw_date} to date')