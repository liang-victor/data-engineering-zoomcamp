from prefect import flow, task
import time

@task
def fetch_data(url):
    print(f"getting_data_from_url: {url}")

    time.sleep(1)
    return [1, 2,3,4,5]


@task
def double_values(values):
    return [2*x for x in values]

@task
def write_data_somewhere(values, location):
    print(f'Writing data to {location}')
    time.sleep(1)
    print("done!")

@flow(name="My first prefect data pipeline" )
def main_flow():
    
    values = fetch_data("www.yahoo.com")
    values = double_values(values)
    write_data_somewhere(values, "my data storage location")

if __name__=='__main__':
    main_flow()

