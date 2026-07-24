import pandas as pd
if __name__ == '__main__':

    data = {
        'match_id':range(1,10)
    }
    df = pd.DataFrame(data)
    print(df.head)