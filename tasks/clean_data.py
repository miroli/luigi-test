import luigi
import pandas as pd
from .fetch_data import FetchData


class CleanData(luigi.Task):
    def requires(self):
        return FetchData()

    def output(self):
        return luigi.LocalTarget('cleaned_data.csv')

    def run(self):
        with self.input().open('r') as f:
            skiprows = [0, 1, 2, 3, 4, 5, 7]
            df = pd.read_csv(f, skiprows=skiprows, na_values='-')
            renames = {'Unnamed: 1': 'Region', 'Candidate': 'Round'}
            df.rename(columns=renames, inplace=True)
            df['Round'] = df['Round'].ffill()

        with self.output().open('w') as f:
            print(df.to_csv(index=False), file=f)
