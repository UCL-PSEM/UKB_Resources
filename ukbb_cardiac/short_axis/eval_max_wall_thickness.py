# Copyright 2025, Nikhil Paliwal. All Rights Reserved.

import os
import argparse
import pandas as pd

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_dir', required=True, help='Path to directory containing subject folders')
    parser.add_argument('--output_csv', required=True, help='Path to output CSV file')
    args = parser.parse_args()

    data_dir = args.data_dir
    out_csv = args.output_csv

    results = []

    for case_id in sorted(os.listdir(data_dir)):
        case_path = os.path.join(data_dir, case_id)
        wt_path = os.path.join(case_path, 'wall_thickness_ED_max.csv')

        if not os.path.exists(wt_path):
            continue

        try:
            df = pd.read_csv(wt_path, index_col=0)
            global_value = df.loc['Global', 'Thickness_Max']
            results.append((case_id, global_value))
        except Exception as e:
            print(f"Failed to process {case_id}: {e}")

    # Save to output CSV
    df_out = pd.DataFrame(results, columns=['Subject_ID', 'Max_Wall_Thickness_Global_mm'])
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df_out.to_csv(out_csv, index=False)

    print(f"Saved global wall thickness summary to: {out_csv}")
