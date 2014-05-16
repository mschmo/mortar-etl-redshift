@outputSchema("wikicodeinfo:tuple(language:chararray, wiki_type:chararray)")
def decode_wikicode(wikicode):
    """
    Split the wikicode (e.g. fr.b) into its separate parts.
    """
    if not wikicode:
        return (None, None)
    wikicode_split = wikicode.split('.')
    if wikicode_split and len(wikicode_split) == 2:
        language = wikicode_split[0]
        wiki_type = wikicode_split[1]
        return (language, wiki_type)
    else:
        return (None, None)

@outputSchema("decode_pageviews:bag{hourly_pageviews:tuple(day:int, hour:int, pageviews:int)}")
def decode_pageviews(pageview_str):
    """
    Decode the wikipedia pageview string format into
    (day, hour, pageviews).

    See _decode_pageview_str for description of how to parse the wikipedia format.
    """
    encoded_pageviews = pageview_str.split(',')
    return [_decode_pageview_str(encoded_pageview) for encoded_pageview in encoded_pageviews]

def _decode_pageview_str(encoded_pageview):
    """
    Pull out the day, hour, and hourly pageviews from the wikipedia log format.

    Wikipedia day and hour are coded as one character each, as follows:
    Hour 0..23 shown as A..X                            convert to number: ordinal (char) - ordinal ('A')
    Day  1..31 shown as A.._  27=[ 28=\ 29=] 30=^ 31=_  convert to number: ordinal (char) - ordinal ('A') + 1

    Returns (hour, day, num_pageviews)
    """
    day_encoded, hour_encoded, pageviews = \
        (encoded_pageview[0],
         encoded_pageview[1],
         encoded_pageview[2:])

    day_decoded  = ord(day_encoded) - ord('A') + 1
    hour_decoded = ord(hour_encoded) - ord('A')

    return (day_decoded, hour_decoded, int(pageviews))
