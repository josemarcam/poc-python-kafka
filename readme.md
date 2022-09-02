# How to use/configure (Python 3.8)

Install dependencies whith pip 

    pip install -r requirements.txt

Change configuration variables at main.py
    SERVER_ADDRESS=<Kafka broker url>:<kafka broker port>

# Produce messages:
    python main.py produce <message>
    // use with --help for more options

# Consume messages:
    python main.py consume 
    // use with --help for more options

# Creating Topics:
    python main.py create-topic 
    // use with --help for more options




    


