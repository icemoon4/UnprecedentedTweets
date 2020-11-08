from google.cloud import language_v1
import csv
from geopy.geocoders import Nominatim

def sample_analyze_sentiment(text_content):
    """
    Analyzing Sentiment in a String

    Args:
      text_content The text content to analyze
    """

    client = language_v1.LanguageServiceClient()

    # text_content = 'I am so happy and joyful.'

    # Available types: PLAIN_TEXT, HTML
    type_ = language_v1.Document.Type.PLAIN_TEXT

    # Optional. If not specified, the language is automatically detected.
    # For list of supported languages:
    # https://cloud.google.com/natural-language/docs/languages
    language = "en"
    document = {"content": text_content, "type_": type_, "language": language}

    # Available values: NONE, UTF8, UTF16, UTF32
    encoding_type = language_v1.EncodingType.UTF8

    response = client.analyze_sentiment(request = {'document': document, 'encoding_type': encoding_type})
    return response 

def main():
    locator = Nominatim(user_agent="myGeocoder")
    with open('collectedtweets.csv', encoding='utf8') as csv_file:
        with open('analyzedtweets.csv', mode='w',encoding='utf8', newline='') as output_file:
            csv_writer = csv.writer(output_file, delimiter=',')
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    csv_writer.writerow(row+["Tweet Sentiment Score", "Tweet Sentiment Magnitude","Location"])
                    print(f'Column names are {", ".join(row)}')
                    line_count += 1
                else:
                    response = sample_analyze_sentiment(row[3])
                    print(row[3])
                    print(u"Tweet sentiment score: {}".format(response.document_sentiment.score))
                    print(u"Tweet sentiment magnitude: {}".format(response.document_sentiment.magnitude))
                    print()
                    location = locator.geocode(row[2])
                    if location is not None:
                        csv_writer.writerow(row+[response.document_sentiment.score, response.document_sentiment.magnitude,"{},{}".format(location.latitude, location.longitude)])
                    else:
                        csv_writer.writerow(row+[response.document_sentiment.score, response.document_sentiment.magnitude,""])

if __name__=="__main__":
    main()

