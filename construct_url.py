from urllib.parse import urlencode, quote_plus



def construct_nitter_url(keyword, since, until, base_url="https://nitter.net"):
    """
    Constructs a Nitter search URL with the given keyword, since date, and until date.
    
    Args:
        keyword (str): The search keyword (e.g., "#الاردن").
        since (str): The start date in "YYYY-MM-DD" format.
        until (str): The end date in "YYYY-MM-DD" format.
        base_url (str): The Nitter instance base URL.
        
    Returns:
        str: The constructed Nitter search URL.
    """

    # Construct query (the part after q=)
    query = f"{keyword} since:{since} until:{until}"

    # Construct full URL
    search_url = f"{base_url.rstrip('/')}/search"
    params = {
        "f": "tweets",
        "q": query,
        "since": "",
        "until": "",
        "near": ""
    }
    url = f"{search_url}?{urlencode(params, quote_via=quote_plus)}"

    return url


