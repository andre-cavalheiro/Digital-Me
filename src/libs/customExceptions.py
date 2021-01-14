class NoMatch(Exception):
    pass

class NoYtChannel(Exception):
    pass

class UnexpectedFBComment(Exception):
    pass

class UnexpectedFBPost(Exception):
    pass

class UnexpectedFBReaction(Exception):
    pass

class Ignore(Exception):
    pass

class NothingToDo(Exception):
    pass

class conflictingQIDs(Exception):
    pass

class ContentWithMultipleLocations(Exception):
    pass

class ContentWithoutLocation(Exception):
    pass