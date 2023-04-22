'''
The data I will use comes from a Hacker News (HN) API that returns JSON 
data of the top stories in 2014.

The Hacker News is a link aggregator website that users vote up stories 
that are interesting to the community. It is similar to Reddit, 
but the community only revolves around on computer science and 
entrepreneurship posts.

The downloaded JSON file contains a single key stories, which contains 
a list of stories (posts). Each post has a set of keys, 
but we will deal only with the following keys:

- created_at: A timestamp of the story's creation time.
- created_at_i: A unix epoch timestamp.
- url: The URL of the story link.
- objectID: The ID of the story.
- author: The story's author (username on HN).
- points: The number of upvotes the story had.
- title: The headline of the post.
- num_comments: The number of a comments a post has.

'''

from pipeline import Pipeline #Import the Pipeline class from the pipeline module
from pipeline import build_csv, Pipeline
from stop_words import stop_words

pipeline = Pipeline() #Instantiate an instance of the Pipeline class to the variable pipeline


from datetime import datetime
import json
import io
import csv
import string

from pipeline import build_csv, Pipeline
from stop_words import stop_words

pipeline = Pipeline()

@pipeline.task()
def file_to_json():
    with open('hn_stories_2014.json', 'r') as f:
        data = json.load(f)
        stories = data['stories']
    return stories

@pipeline.task(depends_on=file_to_json)
def filter_stories(stories):
    def is_popular(story):
        return story['points'] > 50 and story['num_comments'] > 1 and not story['title'].startswith('Ask HN')
    
    return (
        story for story in stories
        if is_popular(story)
    )

@pipeline.task(depends_on=filter_stories)
def json_to_csv(stories):
    lines = []
    for story in stories:
        lines.append(
            (story['objectID'], datetime.strptime(story['created_at'], "%Y-%m-%dT%H:%M:%SZ"), story['url'], story['points'], story['title'])
        )
    return build_csv(lines, header=['objectID', 'created_at', 'url', 'points', 'title'], file=io.StringIO())

@pipeline.task(depends_on=json_to_csv)
def extract_titles(csv_file):
    reader = csv.reader(csv_file)
    header = next(reader)
    idx = header.index('title')
    
    return (line[idx] for line in reader)

@pipeline.task(depends_on=extract_titles)
def clean_title(titles):
    for title in titles:
        title = title.lower()
        title = ''.join(c for c in title if c not in string.punctuation)
        yield title

@pipeline.task(depends_on=clean_title)
def build_keyword_dictionary(titles):
    word_freq = {}
    for title in titles:
        for word in title.split(' '):
            if word and word not in stop_words:
                if word not in word_freq:
                    word_freq[word] = 1
                word_freq[word] += 1
    return word_freq

@pipeline.task(depends_on=build_keyword_dictionary)
def top_keywords(word_freq):
    freq_tuple = [
        (word, word_freq[word])
        for word in sorted(word_freq, key=word_freq.get, reverse=True)
    ]
    return freq_tuple[:100]

ran = pipeline.run()
print(ran[top_keywords])