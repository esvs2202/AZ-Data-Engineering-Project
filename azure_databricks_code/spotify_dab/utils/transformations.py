class Reusable:

    def __init__(self):
        pass
    
    def dropColumns(self, df, columns):
        df = df.drop(*columns)
        return df
    def deDuplicate(self, df, columns):
        df = df.dropDuplicates(columns)
        return df
    def replaceStr(self,oldStr, newStr):
        return lambda s: s.replace(oldStr, newStr)

    
