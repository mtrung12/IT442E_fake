import pandas as pd
from sklearn.preprocessing import StandardScaler
import pickle

class DataProcessor:
    def __init__(self):
        self.add_cols = ['street', 'ward', 'district', 'city', 'house_type', 'property_legal_document']
        self.scaler = StandardScaler()
        self.global_mean = 0
        self.encode_mean = {}
        self.features = []
        
    def scale(self, train_X, train_y):
        self.global_mean = train_y.mean()
        
        df = train_X.copy()
        df['price'] = train_y    
        
        for col in self.add_cols:
            means = df.groupby(col)['price'].mean()
            self.encode_mean[col] = means.to_dict()
            df[col] = df[col].map(means) 
            df[col] = df[col].fillna(self.global_mean)
        self.scaler.fit(df.drop(columns=['price']))
        self.features = df.drop(columns=['price']).columns.tolist()
        
        return self
        
    def transform(self, X):
        target_X = X.copy()
        for col in self.add_cols:
            target_X[col] = target_X[col].map(self.encode_mean.get(col))
            target_X[col] = target_X[col].fillna(self.global_mean)
        scaled_target_X = self.scaler.transform(target_X)
        
        return pd.DataFrame(scaled_target_X, columns=self.features, index=target_X.index)
    
    def save(self, filepath):
        with open(filepath, 'wb') as f:
            pickle.dump(self, f)