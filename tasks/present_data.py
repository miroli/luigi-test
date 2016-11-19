import luigi
import pandas as pd
from .clean_data import CleanData


class PresentData(luigi.Task):
    candidate = luigi.Parameter()

    def requires(self):
        return CleanData()

    def output(self):
        return luigi.LocalTarget('presentation_data.md')

    def run(self):
        with self.input().open('r') as f:
            df = pd.read_csv(f)

        filters = (df['Region'] == 'FRANCE') & (df['Round'] == 1.0)
        val = df[self.candidate][filters].values[0]

        with self.output().open('w') as f:
            f.write('#French Presidential Election 2012 Report\n\n')
            info = '{} received {} of the popular vote\n'
            f.write(info.format(self.candidate, val))