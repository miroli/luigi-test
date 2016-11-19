import luigi
import pandas as pd


class FetchData(luigi.Task):
    def requires(self):
        pass

    def output(self):
        return luigi.LocalTarget('fetched_data.csv')

    def run(self):
        url = ('http://129.177.90.166/webview/velocity?headers=par_name'
               '&par_nameslice=5&stubs=round&stubs=nuts_id&measure=common'
               '&virtualslice=pv_p_value&nuts_idsubset=FR+-+FR1%2CFR2%2C'
               'FR3%2CFR4%2CFR5%2CFR6%2CFR7%2CFR8%2CFR9%2CFRX%2CFRZ&layers'
               '=virtual&mode=cube&nuts_idslice=FR1&virtualsubset='
               'pv_p_value&roundsubset=1+-+2&measuretype=4&roundslice=1'
               '&cube=http%3A%2F%2F129.177.90.166%3A80%2Fobj%2FfCube%2F'
               'FRPR2012_C1&view=methodSources&par_namesubset=2%2C5'
               '%2C7+-+8%2C10&top=yes&executespreadsheet=true'
              )
        df = pd.read_excel(url)
        with self.output().open('w') as f:
            print(df.to_csv(index=False), file=f)
