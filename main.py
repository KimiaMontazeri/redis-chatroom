from datetime import datetime

import redis
import json

r = redis.Redis(host="127.0.0.1", port=6379, db=0, password='password', decode_responses=True)

user = input('Please enter your username: ')
groups = r.smembers(user + ":groups")

while True:
    command = input('1) create group\n2) groups\n3) write a message\n')

    if command == '1':
        group = input('please enter group name: ')
        creator = user
        created_at = str(datetime.now())
        description = input('please enter group description: ')
        members = input('please enter group members (including yourself!) comma seperated: ')
        load_last_n_hour_messages = input('please enter last n hour to load messages: ')

        groupJSON = {
            "creator": creator,
            "created_at": created_at,
            "description": description,
            "members": members.split(','),
            "load_last_n_hour_messages": load_last_n_hour_messages
        }

        r.setnx(group, json.dumps(groupJSON))

        for member in members.split(','):
            r.sadd(member + ":groups", group)
    elif command == '2':
        groups = r.smembers(user + ":groups")
        pipe = r.pipeline()
        for group in groups:
            pipe.keys(group+'-*')

        keys = pipe.execute()

        query = []
        for key_list in keys:
            for key in key_list:
                query.append(key)
                pipe.get(key)

        messages = pipe.execute()
        for index, message in enumerate(messages):
            print(query[index], message)

        sub = r.pubsub()

        print("------------------------------------------------------------------\nGroups list: ")
        for group in groups:
            print(group, " ==> ", json.loads(r.get(group))['members'])
            sub.subscribe(group + ":messages")

        for message in sub.listen():
            if message is not None:
                print(message.get('data'))

    elif command == '3':
        groups = r.smembers(user + ":groups")
        group = input("please choose your group: ")
        if group not in groups:
            print("you are not a member of this group!")
        else:
            message = input("please write your message: ")
            print(group)
            last_n_hour = int(json.loads(r.get(group))['load_last_n_hour_messages'])

            dt = datetime.now()

            r.publish(group + ":messages", group + '-' + user + '-' + str(dt) + ': ' + message)
            r.set(group + '-' + user + '-' + str(dt), message, ex=last_n_hour * 3600)

    else:
        exit(0)
