# ATproto labeler

This is an implementation of a basic labeler. It does not have any UI and does not impose any workflow on you.
Labels can be created via an API, or any other way you implement.

## Getting started

1. Clone this repository.
2. Copy `example.env` to `.env` and at the very least uncomment and edit `DATA_DIR` variable there.
3. Copy `example_config.yaml` to `config.yaml`. Generate and put there a private key. Edit or simply remove everything after the line with the private key.
4. Run `docker compose up --build -d`

Congratulations! You've got yourself a perfectly useless labeler :) (by default there's no way to create any labels)

## Enabling a simple API

Copy `docker-compose.override.example.yaml` to `docker-compose.override.yaml` and run `docker compose up -d`.

Now you can create labels by sending them as a POST request to http://127.0.0.1:8081/label, e.g.:

```sh
curl -X POST --json '{"uri": "did:plc:foobar","val": "!hide"}' http://127.0.0.1:8081/label
```

Note that there's no authentication whatsoever, so you should not expose this port to outside world.
This API is intended only as an example. If you insist on using it anyway - at least put it behind
a reverse proxy with authentication.

## Setting up the labeler account to actually work

For someone to be able to subscribe to your labeler and see the labels, two things need to happen:

1. Labeler service record need to exist in the PDS.
2. DID document needs to have labeler's signing key and service endpoint.

### Updating PLC

1. Ensure that you have `did`, `password`, `private_key` and `endpoint` set in your config.
2. While your labeler is running, run `docker compose exec labeler ./update-plc --config=/config.yaml`.
3. If any changes are needed, you will see a message stating that. Wait for the email with the token and re-run the same command, but add `--token` flag

### Updating labeler service record

`labeler` and `list-labeler` automatically do it at startup. Just make sure that in your config
you have `labels` set up the way you want them, and that `did` and `password` are specified too.

## Labeling accounts based on a mute list

There's an implementation of a labeler that takes a list and converts it into a label in `cmd/list-labeler` directory.

1. Add `lists` entry to your config file. It should be a mapping from label name to a list URI.
2. Copy `docker-compose.override.example.yaml` to `docker-compose.override.yaml`, if you haven't yet.
3. Uncomment `entrypoint` line and comment out admin API from both `ports` and `command` sections.
4. Ensure that you have `did` and `password` set in your config file.
5. Run `docker compose up -d`.

One caveat is that it uses `app.bsky.graph.getList` call to fetch the list members. Due to that, if someone blocks your account - they won't be returned in the response and `list-labeler` would think that they have been removed from the list.

## Further customization

You can use `cmd/labeler` as a starting point for implementing your own labeler. You don't necessarily even need to fork this repo. Just copy `cmd/labeler/main.go` and import `bsky.watch/labeler` module.
