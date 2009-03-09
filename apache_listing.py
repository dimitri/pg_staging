##
## HTML Parser for apache listings, with basic matching
##

from HTMLParser import HTMLParser

class ApacheListingParser(HTMLParser):
    """ parses HTML listing given by Apache mod_dir """

    def __init__(self, content, pattern):
        """ buffer is a file-like object, contains the HTML source """
        self.buffering = False
        self.buffer    = ""
        self.content   = content
        self.pattern   = pattern
        HTMLParser.__init__(self)

        self.current_tag    = None
        self.current_bullet = None

        self.files = []

    def parse(self):
        for line in self.content:
            self.feed(line)

        return self.files
    
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.buffering = True

        self.current_tag = tag

    def handle_endtag(self, tag):
        if self.current_tag == 'a':
            if self.buffer.find(self.pattern) > -1:
                self.files.append(self.buffer)

        # clean up for next tag data
        self.current_tag = None
        self.buffer = ""

    def handle_data(self, data):
        if self.buffering:
            if data.strip('\r\n') != '':
                self.buffer += data
        
            
