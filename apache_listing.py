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
        self.current_file   = None
        self.saw_a          = False
        self.n_td_since_a   = 0

        # [('filename', 'size'), ...]
        self.files = []

    def parse(self):
        for line in self.content:
            self.feed(line)

        return self.files
    
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.buffering = True
            self.saw_a     = True

        if tag == 'td':
            if self.saw_a:
                self.n_td_since_a += 1

            if self.n_td_since_a == 2:
                self.buffering = True

        self.current_tag = tag

    def handle_endtag(self, tag):
        if self.current_tag == 'a':
            if self.buffer.find(self.pattern) > -1:
                self.current_file = self.buffer

            self.buffering = False

        if self.current_tag == 'td':
            if self.buffering and self.current_file:
                self.files.append((self.current_file, self.buffer))

            self.buffering = False

        if tag == 'tr':
            self.n_td_since_a = 0
            self.current_file = None
            self.saw_a        = False

        # clean up for next tag data
        self.current_tag = None
        self.buffer      = ""

    def handle_data(self, data):
        if self.buffering:
            if data.strip('\r\n') != '':
                self.buffer += data
        
            
