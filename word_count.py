import nltk
from nltk.tokenize import word_tokenize

nltk.download('punkt', quiet=True)

def word_count(text):
    words = word_tokenize(text)
    return len(words)

if __name__ == "__main__":
    user_input = input("Enter text: ")
    count = word_count(user_input)
    print(f"Word count: {count}")