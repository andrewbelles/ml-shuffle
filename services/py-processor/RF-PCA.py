# usr/bin/env python3 
# 
# RF-PCA.py  Andrew Belles  Sept 15th, 2025 
# 
# Loads .parquet data from path into dataframe. 
# Performs random forest and pca to reduce dataset dimensionality 
# 

import argparse
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier 
from sklearn.metrics import roc_auc_score 
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA 

BATCH_SIZE: int = 256 
rng  = np.random.default_rng(0)
SEED = rng.integers(1000000)

def permute_columns(data: pd.DataFrame, rng) -> pd.DataFrame: 

    permuted = pd.DataFrame(
        {c: rng.permutation(data[c].values) for c in data.columns}, 
        columns=data.columns
    )
    return permuted 


class Model(): 

    def __init__(self, path: str, noise_ratio: float = 1.0):
        '''
        Important things to note, this loads the entire model into memory, 
        I will consider an approach later that avoids this in favor of better pipelining 
        '''
        self.data = pd.read_parquet(path, engine="pyarrow")
        self.data = self.data.apply(pd.to_numeric, errors="coerce").astype(np.float32)

        # Pre-split before noise 
        train_real, test_real = train_test_split(
            self.data,  
            test_size=0.2, 
            random_state=SEED, 
            shuffle=True 
        )

        if not isinstance(train_real, pd.DataFrame): 
            raise ValueError("Training data failed to generate as DataFrame")

        if not isinstance(test_real, pd.DataFrame): 
            raise ValueError("Test data failed to generate as DataFrame")

        train_median = train_real.median(numeric_only=True)
        print(f"median={train_median}")
        train_real = train_real.fillna(train_median).astype(np.float32)
        test_real  = test_real.fillna(train_median).astype(np.float32)

        train_noise_ct = int(noise_ratio * len(train_real))
        test_noise_ct  = int(noise_ratio * len(test_real))

        train_noise = permute_columns(train_real, rng).sample(
            n=train_noise_ct, 
            replace=(train_noise_ct > len(train_real)),
            random_state=rng
        )

        test_noise = permute_columns(test_real, rng).sample(
            n=test_noise_ct, 
            replace=(test_noise_ct > len(test_real)),
            random_state=rng
        )

        self.real  = train_real 
        self.train = pd.concat([train_real, train_noise], ignore_index=True)
        self.train_labels = np.concatenate(
            [np.ones(len(train_real)), 
             np.zeros(len(train_noise))]
        )
        self.test = pd.concat([test_real, test_noise], ignore_index=True)
        self.test_labels = np.concatenate(
            [np.ones(len(test_real)), 
             np.zeros(len(test_noise))]
        )

        p = rng.permutation(len(self.train))
        self.train = self.train.iloc[p].reset_index(drop=True)
        self.train_labels = self.train_labels[p]

        p = rng.permutation(len(self.test))
        self.test = self.test.iloc[p].reset_index(drop=True)
        self.test_labels = self.test_labels[p]

        self.rf_   = None 
        self.path_ = path 


    def rf_train(self, n_estimators=600, max_depth=None, 
                 min_samples_leaf=5, max_features="sqrt"): 
        '''
        From our contrastive dataset, train a random forest model to determine which 
        labels are real and which are randomized noise. 

        '''
        rf = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_leaf=min_samples_leaf,
            max_features=max_features,
            bootstrap=True, 
            oob_score=True, 
            n_jobs=-1,
            class_weight="balanced_subsample",
            random_state=SEED,
            verbose=1 
        )

        rf.fit(self.train, self.train_labels)
        self.rf_ = rf 
        return rf 


    def rf_eval(self, panic=False):
        if self.rf_ is None: 
            if not panic:
                raise RuntimeError("No random forest model trained.")
            else: 
                self.rf_ = self.rf_train() # Makes typechecker happy 
        
        oob = getattr(self.rf_, "oob_score_", None)
        probability = np.asarray(self.rf_.predict_proba(self.test))[:, 1]
        
        auc = roc_auc_score(self.test_labels, probability)
        return {"oob_score": oob, "test_auc": auc}


    def reduce(self):
        if self.rf_ is None: 
            self.rf_train() 

        importances = self.rf_.feature_importances_
        feature_names = self.rf_.feature_names_in_ 
        features = pd.DataFrame({"Feature": feature_names, "Importance": importances})

        features = features.sort_values(by='Importance', ascending=False)
        top_features = features['Feature'][:512].values

        # Dataset to perform randomized PCA upon 
        scaler = StandardScaler(with_mean=True, with_std=True)
        X = self.real[top_features].to_numpy(dtype=np.float32)
        Z = scaler.fit_transform(X)
        self.pca = PCA(n_components=512, svd_solver='randomized', random_state=SEED) 

        _ = self.pca.fit_transform(Z) 

        # Ensure data has real structure
        eigenvalues = self.pca.explained_variance_ 
        norm_ev     = self.pca.explained_variance_ratio_
        cumulative  = np.cumsum(norm_ev)

        col_ct = Z.shape[1]
        harmonic = np.cumsum(1.0 / np.arange(1, col_ct+1)[::-1])[::-1] / col_ct 
        bs = harmonic[:512] # Broken stick expectation 

        k_var = int(np.searchsorted(cumulative, 0.95) + 1)
        k_var = min(k_var, 512)
        below = np.where(norm_ev < bs)[0]

        k_mark = 64

        n = np.arange(1, 513)
        _, ax = plt.subplots(1, 2)
        ax[0].plot(n[n <= k_mark], eigenvalues[:k_mark], marker="o", linestyle="-",
                   label=f"PC 1-{k_mark}", color="tab:orange") 
        ax[0].plot(n[n > k_mark], eigenvalues[k_mark:], marker="o", linestyle="-",
                   label=f"PC {k_mark+1}-512", color="tab:blue")
        ax[0].axvline(k_mark, color="gray", linestyle=":", alpha=0.8)
        ax[0].set_xlabel("Component")
        ax[0].set_ylabel("Eigenvalue")
        ax[0].legend(loc="best")

        ax2 = ax[0].twinx()
        ax2.plot(n, norm_ev, linestyle="--")
        ax2.plot(n, bs, linestyle=":", alpha=0.8)
        ax2.set_ylabel("explained variance ratio / broken-stick expectation")

        ax[1].plot(n[n <= k_mark], cumulative[:k_mark], marker="o", color="tab:orange",
                   label=f"cumulative EVR (1-{k_mark})")
        ax[1].plot(n[n > k_mark], cumulative[k_mark:], marker="o", color="tab:blue", 
                   label=f"cumulative EVR ({k_mark+1}-(512)")
        ax[1].axhline(0.95, color="gray", linestyle=":")
        ax[1].axvline(k_var, color="gray", linestyle=":")
        ax[1].set_ylim(0, 1.01)
        ax[1].set_xlabel("Component")
        ax[1].set_ylabel("Cumulative explained variance")
        plt.title("PCA Results + Check against Broken-stick Expectation")
        plt.tight_layout()
        
        plt.savefig('pca_graph.png')
        plt.close() 


def main():
    parser = argparse.ArgumentParser() 
    parser.add_argument("--path", default="../data/matrix.parquet")

    args  = parser.parse_args()
    model = Model(args.path)

    model.reduce()

if __name__ == "__main__":
    main()
