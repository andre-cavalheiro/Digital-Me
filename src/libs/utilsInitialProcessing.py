import logging

import pandas as pd
import tldextract

from libs.customExceptions import NoMatch, UnexpectedFBComment, Ignore, UnexpectedFBPost, NothingToDo


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
            logging.error(ex)

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

            return type.strip(), 'Commented', author.strip()
    raise NoMatch


def extractContextFromFbSelfPosts(text):
    '''

    :param text:
    :return: (actionPerformedByUser, targetContentType, facebookLocation, extraTags)
    '''
    for action in ['posted', 'shared', 'updated', 'created a poll', 'wrote', 'likes', 'commented',
                   'added', 'reviewed', 'updated', 'was with']:
        if action in text:
            text2 = text.split(action)[-1]  # Drop useless stuff from the beginning
            text3 = text2.split(' ')[2:]    # Drop preposition 'in', 'on', 'a' ...
            text3 = ' '.join(text3).capitalize()[:-1]  # Set first char to upper case, remove final dot
            r=1

            if action == 'posted':
                return action.capitalize().strip(), 'Post', text3.strip(), []

            elif action == 'shared':
                if 'timeline' in text3:
                    contentType = text3.split(' ')[0]
                    friend = ' '.join(text3.split(' ')[2:]).split("\'s timeline")[0:-1][0]
                    friendTimeline = ' '.join(text3.split(' ')[2:])
                    return action.capitalize().strip(), contentType.strip(), friendTimeline.strip(), [friend.title().strip()]
                else:
                    return action.capitalize().strip(), text3.strip(), 'Self Timeline', []

            elif action == 'wrote':
                friend = text3.split("\'s timeline")[0:-1][0]   # Get friend's full name
                return 'Posted', 'Post', text3.strip(), [friend.title().strip()]

            elif action == 'created a poll':
                breakpoint()
                return action.capitalize().strip(), 'Poll', text3.strip(), []

            elif action == 'updated' or action == 'was with':
                # raise NothingToDo
                return 'Posted', 'Post', 'Self Timeline', []

            elif action == 'reviewed' or action == 'added' or action == 'commented' or action == 'likes':
                raise Ignore    # Some of these have actual value but too complex for the initial phase
                # Comments, or likes shouldn't be in this dataset - required investigation to understand how they work.

            else:
                raise Exception('Implementation error here')
    raise NoMatch


def extractContextFromFbOthersPosts(text):
    '''

    :param text:
    :return: (actionPerformedByUser, targetContentType, facebookLocation, targetContentAuthor)
    '''
    for action in ['shared', 'wrote', 'added', 'reviewed', 'updated', 'was with']:
        if action in text:
            aux = text.split(action)
            author, text2 = aux[0], aux[-1]
            text3 = text2.split(' ')[2:]    # Drop preposition 'in', 'on', 'a' ...

            if action == 'shared':
                contentType = text3[0].capitalize()
                return action.capitalize().strip(), contentType.strip(), 'Self Timeline', author.title().strip()

            elif action == 'wrote':
                return action.capitalize().strip(), 'Post', 'Self Timeline', author.title().strip()

            elif action == 'added':
                # Only usecase in personal database is 'added new photo'
                contentType = ' '.join(text3[:2]).capitalize()
                return action.capitalize().strip(), contentType.strip(), 'Self Timeline', author.title().strip()

            elif action == 'reviewed' or action == 'commented' or action == 'likes':
                raise Ignore    # Some of these have actual value but too complex for the initial phase
                # Comments, or likes shouldn't be in this dataset - required investigation to understand how they work.

            else:
                raise Exception('Implementation error here')
    raise NoMatch


def extractContextFromReactions(text):
    # "Andr\u00c3\u00a9 Cavalheiro likes Miguel Lino's post in SWAMP."
    r=1
    for action in ['likes', 'liked', 'reacted']:
        if action in text:
            text2 = text.split(action)[-1]  # Drop useless stuff from the beginning

            if len([s for s in [' to a ', ' his own '] if s in text2]) > 0:
                # 'to a' means there is no info reggarding target content type e.g. '<User> reacted to a post'
                # 'his own' means user reacted to his own content - not relevant
                raise Ignore

            if action == 'likes' or action == 'liked' or action == 'reacted':
                aux = text2.split('\'s')
                targetContentAuthor, targetContentType = aux[0], aux[-1]

                if ' in ' in targetContentType:
                    aux = targetContentType.split(' in ')
                    targetContentType, facebookLocation = aux[0], aux[1]
                    facebookLocation = facebookLocation.split('.')[0]     # Remove final dot
                else:
                    targetContentType = targetContentType.split('.')[0]     # Remove final dot
                    facebookLocation = None
                return targetContentType.strip(), targetContentAuthor.strip(), facebookLocation.strip()
            else:
                raise Exception('Implementation error here')
    raise NoMatch


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