import pandas as pd
from libs.customExceptions import NoMatch, UnexpectedFBComment, Ignore, UnexpectedFBPost
import tldextract


def dropByPercentageJSON(data, percentage):
    assert(percentage>=0 and percentage<=1)
    stopIndex = int((1-percentage)*len(data))
    droppedData = data[stopIndex:]
    data = data[:stopIndex]
    return data, droppedData


def dropByPercentageDF(df, percentage):
    stopIndex = int(df.shape[0] * (1-percentage))
    droppedDf = df.iloc[stopIndex:, :]
    df = df.iloc[:stopIndex, :]
    return df, droppedDf


def loop(data):
    iterator = -1
    while iterator+1 < len(data):
        iterator += 1
        currentItem = data[iterator]
        yield iterator, currentItem


def fromDictToDf(data):
    dataIterator = loop(data)
    df = None

    while True:
        try:
            iterator, currentItem = next(dataIterator)
            currentDf = pd.DataFrame.from_dict(currentItem, orient='index').T
            if df is None:
                df = currentDf
            else:
                df = pd.concat([df, currentDf],  sort=True)
        except StopIteration:
            break
        except Exception as ex:
            print(ex)

    df = df.reset_index(drop=True)
    return df


def detectAndExtractSubstrings(substrings, text):
    for s in substrings:
        if s in text:
            matchedSubstring = s
            payload = text.replace(matchedSubstring, '').strip()
            return matchedSubstring, payload
    raise NoMatch


def extractContextFromFBComment(text):

    for action in ['commented on', 'replied to']:
        if action in text:
            text2 = text.split(action)[-1]  # Drop useless stuff

            if 'his own' in text2:
                author = 'Self'
                type = text2.split(" ")[-1].capitalize()[:-1]  # Set first char to upper case, remove final dot
            else:
                remainingText = text2.split("\'s ")
                author = remainingText[0]
                type = remainingText[-1].capitalize()[:-1]  # Set first char to upper case, remove final dot

            """
            if ' post' in text2:
                author = text2.split("\'s post.")[0] if 'his own' not in text2 else 'self'
                type = 'Post'
            elif ' comment' in text2:
                author = text2.split("\'s comment.")[0] if 'his own' not in text2 else 'self'
                type = 'Comment'
            elif ' photo' in text2:
                author = text2.split("\'s photo.")[0] if 'his own' not in text2 else 'self'
                type = 'photo'
            elif ' photo' in text2:
                author = text2.split("\'s photo.")[0] if 'his own' not in text2 else 'self'
                type = 'photo'
            elif ' video' in text2:
                author = text2.split("\'s photo.")[0] if 'his own' not in text2 else 'self'
                type = 'video'
            else:
                raise UnexpectedFBComment
            """

            return type, author
    raise NoMatch


def extractContextFromFBPosts(text):
    for action in ['posted', 'shared', 'updated', 'was with', 'reviewed', 'updated', 'created a poll',
                   'wrote', 'added', 'likes', 'commented']:
        if action in text:
            text2 = text.split(action)[-1]  # Drop useless stuff from the beggining
            text3 = text2.split(' ')[2:]    # Drop preposition 'in', 'on', 'a' ...
            text3 = ' '.join(text3).capitalize()[:-1]  # Set first char to upper case, remove final dot
            r=1
            if action == 'posted':
                return action, text3

            elif action == 'shared':
                return action, 'Timeline'   # text3 contains type of content that was shared> link, image, post ...

            elif action == 'was with':
                return action, text2    # text2 !!

            elif action == 'created a poll':
                return action, text3

            elif action == 'likes':
                return action, None

            elif action == 'commented':
                return action, None

            elif action == 'wrote':
                text4 = text3.split("\'s timeline")[0:-1]
                return 'wrote on friend\'s timeline', text4

            elif action == 'reviewed' or action == 'updated' or action == 'added':
                raise Ignore

            else:
                raise Exception('Implementation error here')
    raise NoMatch

    return text

def cleanUrl(url):
    '''
    :param url: Any URL
    :return: subdomain.domain.suffix if subdomain is found, else simply domain.suffix
    '''
    parsedUrl = tldextract.extract(url)
    cleaned = '.'.join(part for part in parsedUrl if part)
    # website = parsedUrl.domain
    # subdomain = parsedUrl.subdomain
    # suffix = parsedUrl.suffix
    return cleaned


def parseObj(obj):
    # Facebook files have been badly encoded - saw fix here: https://stackoverflow.com/questions/50008296/facebook-json-badly-encoded

    if isinstance(obj, str):
        return obj.encode('latin_1').decode('utf-8')

    if isinstance(obj, list):
        return [parseObj(o) for o in obj]

    if isinstance(obj, dict):
        return {key: parseObj(item) for key, item in obj.items()}

    return obj