import pandas as pd

class DataProcessor:
    def __init__(self, df=None):
        self.df = df


    def process_all(self):
        self.translate_columns()
        self.fix_datas()
        return self.df


    def translate_columns(self):
        translate = {
            "country": "País",
            "Purchasing Power Value": "Valor do Poder de Compra",
            "Purchasing Power Category": "Categoria do Poder de Compra",        
            "Safety Value": "Valor da Segurança",
            "Safety Category": "Categoria da Segurança",
            "Health Care Value": "Valor do Cuidado com a Saúde",
            "Health Care Category": "Categoria do Cuidado com a Saúde",
            "Climate Value": "Valor do Clima",
            "Climate Category": "Categoria do Clima",
            "Cost of Living Value": "Valor do Custo de Vida",
            "Cost of Living Category": "Categoria do Custo de Vida",
            "Property Price to Income Value": "Valor da Relação Preço da Propriedade/Renda",
            "Property Price to Income Category": "Categoria da Relação Preço da Propriedade/Renda",
            "Traffic Commute Time Value": "Valor do Tempo de Deslocamento no Trânsito",
            "Traffic Commute Time Category": "Categoria do Tempo de Deslocamento no Trânsito",
            "Pollution Value": "Valor da Poluição",
            "Pollution Category": "Categoria da Poluição",
            "Quality of Life Value": "Valor da Qualidade de Vida",
            "Quality of Life Category": "Categoria da Qualidade de Vida"
        }

        if self.df is None:
            raise ValueError("Nenhum DataFrame foi fornecido para traduzir.")

        self.df = self.df.rename(columns=translate)
        return self.df


    def fix_datas(self):
        if self.df is None:
            raise ValueError("Nenhum DataFrame foi fornecido para ser corrigido.")

        for series in self.df.columns:
            handler = self.get_handler(self.df[series])
            self.df[series] = handler.process(self.df[series])

        return self.df

    def remove_columns_nocorr(self):
        if self.df is None:
            raise ValueError("Nenhum DataFrame foi fornecido para remover colunas.")

        for i in self.df.columns:
            if self.df[i].dtype == object:
                self.df = self.df.drop(columns=[i])

        print(f"Fim do drop de colunas não relacionais para correlação.")
        return self.df

    def get_handler(self, series):
        if 'Valor' in series.name:
            return FloatColumnHandler()
        else:
            return ObjectColumnHandler()


class ColumnHandler:
    def process(self, series):
        raise NotImplementedError("Subclasses devem implementar este método")


class FloatColumnHandler(ColumnHandler):
    def process(self, series):
        series = series.replace("'", "", regex=True).replace(":", "", regex=True).replace(",", "", regex=True)
        series = pd.to_numeric(series, errors='coerce').fillna(0.0)
        return series


class ObjectColumnHandler(ColumnHandler):
    def process(self, series):
        series = series.replace("'", "", regex=True).replace(":", "", regex=True)
        series = series.fillna("No Data")
        return series