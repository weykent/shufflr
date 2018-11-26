import click
import json
import pendulum
import pytumblr
import sys
import tqdm

import distributr


def fetch_queue(client, blog_name, offset=0):
    query_params = {'offset': offset}
    progress = tqdm.tqdm(desc='Fetching posts', unit=' posts')
    progress.refresh()
    while True:
        results = client.queue(blog_name, **query_params)
        progress.update(len(results['posts']))
        yield from results['posts']
        if '_links' not in results:
            return
        query_params.update(results['_links']['next']['query_params'])

@click.command()
@click.option('--offset', '-o', type=int, default=0)
@click.argument('credentials_file', type=click.File())
@click.argument('blog_name')
def main(credentials_file, blog_name, offset):
    with credentials_file:
        creds = json.load(credentials_file)

    client = pytumblr.TumblrRestClient(
        creds['consumer_key'],
        creds['consumer_secret'],
        creds['token'],
        creds['token_secret'],
    )

    queue_by_id = {p['id']: p for p in fetch_queue(client, blog_name, offset)}
    api_url = '/v2/blog/{}/post/edit'.format(blog_name)
    distributed = list(distributr.distribute(queue_by_id))
    for id, post_at in tqdm.tqdm(distributed, desc='Updating posts', unit=' posts'):
        original_post_at = pendulum.from_timestamp(int(queue_by_id[id]['scheduled_publish_time']))
        if post_at == original_post_at:
            continue
        client.send_api_request(
            'put', api_url,
            params=dict(id=id, publish_on=post_at.isoformat()),
            valid_parameters=['id', 'publish_on'])

main()
