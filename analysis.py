import json
import pathlib
import sys

from collections import Counter

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


def main(data_dir):
    print("data directory: ", data_dir)
    rows = []
    for data_file in pathlib.Path(data_dir).glob("*.json"):
        with data_file.open("r") as f:
            event_stream = json.load(f)
        rows.extend(tabulate(event_stream))
    df = pd.DataFrame(rows)
    df.start = pd.to_datetime(df.start)
    df.end = pd.to_datetime(df.end)
    print(df)
    df.to_csv("table.csv", index=False)
    button_count(df)
    gap_time(df)
    clock(df)
    ngrams(df)


def button_count(df):
    button_hist = df.loc[
        (df.species == "canis familiaris") &
        (~df.content.str.contains("OTHER")) &
        (~df.content.str.contains(" or ")) &
        (df.content != "") &
        (df.content != "null") &
        (df.content.str.len() < 10) &
        (df.event_type == "button_press")]\
        .content\
        .value_counts()
    button_hist = button_hist.iloc[::-1]
    print(button_hist)
    fig = plt.gcf()
    fig.set_size_inches(18.5, 10.5)
    plt.barh(button_hist.index, button_hist.values)
    plt.savefig("data/button_count")


def gap_time(df):
    def _gap_time(df):
        df = df.sort_values("start")
        df["transition"] = df.species.shift(1, fill_value=None) == df.species
        df["gap"] = df.start - df.end.shift(1, fill_value=None)
        df["gap"] = df.iloc[1:].loc[df.transition,"gap"].dt.total_seconds()
        return df
    gaps = df.groupby("file_id").apply(_gap_time)
    total = gaps.copy()
    total["species"] = "total"
    gaps = pd.concat([gaps, total])
    def rename(species):
        if species == "homo sapeins":
            return "human -> dog"
        elif species == "canis familiaris":
            return "human -> dog"
        return species
    gaps.species = gaps.species.apply(rename)
    plt.clf()
    ax = sns.swarmplot(data=gaps, x="species", y="gap", color='k', size=3, zorder=0)

    ax.set_xlabel("species")
    ax.set_ylabel("gap time between turns (seconds)")
    ax.set_title("Turn-taking gap time (seconds)")
    plt.savefig("data/gaps.png")


def clock(df):
    # taken from http://qingkaikong.blogspot.com/2016/04/plot-histogram-on-clock.html
    sns.set_context('poster')
    sns.set_style('white')
    N = 23
    bottom = 2

    # create theta for 24 hours
    theta = np.linspace(0.0, 2 * np.pi, N, endpoint=False)


    # width of each bin on the plot
    width = (2*np.pi) / N

    # make a polar plot
    plt.figure(figsize = (12, 8))
    ax = plt.subplot(111, polar=True)

    # make the histogram that bined on 24 hour
    radii, tick = np.histogram(df[df.species == "canis familiaris"].start.dt.hour, bins = N)
    bars = ax.bar(theta, radii, width=width, bottom=bottom)

    # set the lable go clockwise and start from the top
    ax.set_theta_zero_location("N")
    # clockwise
    ax.set_theta_direction(-1)

    # set the label
    ticks = ['0:00', '3:00', '6:00', '9:00', '12:00', '15:00', '18:00', '21:00']
    ax.set_xticklabels(ticks)
    ax.set_title("Canine presses by time of day")
    plt.tight_layout()
    plt.savefig("data/press_by_hour.png")


def ngrams(df):
    tokens = df.loc[
        (df.species == "canis familiaris") &
        (~df.content.str.contains("OTHER")) &
        (~df.content.str.contains(" or ")) &
        (df.content != "") &
        (df.content.str.len() < 10) &
        (df.event_type == "button_press")]
    ngrams = []
    for _, gdf in tokens.groupby("file_id"):
        for prev, curr in zip(gdf.content, gdf.content.iloc[1:]):
            ngrams.append(tuple(sorted([prev, curr])))
    count = Counter(ngrams)
    with open("data/canine_ngrams.csv", 'w') as f:
        for k,v in  count.most_common():
            f.write(f"{k},{v}\n")

    tokens = df.loc[
        (df.species == "homo sapiens") &
        (~df.content.str.contains("OTHER")) &
        (~df.content.str.contains(" or ")) &
        (df.content != "") &
        (df.content != "null") &
        (df.content.str.len() < 10) &
        (df.event_type == "button_press")]
    ngrams = []
    for _, gdf in tokens.groupby("file_id"):
        for prev, curr in zip(gdf.content, gdf.content.iloc[1:]):
            ngrams.append(tuple(sorted([prev, curr])))
    count = Counter(ngrams)
    with open("data/human_ngrams.csv", 'w') as f:
        for k,v in  count.most_common():
            f.write(f"{k},{v}\n")


def tabulate(event_stream):
    rows = []
    file_id = event_stream["id"]
    provenance = event_stream["provenance"]
    start = event_stream["start"]
    end = event_stream["end"]
    agent2species = {
        agent["id"]: agent["species"]
        for agent in event_stream["agents"]
    }
    for event in event_stream["events"]:
        rows.append({
            "file_id": file_id,
            "provenance": provenance,
            "file_start": start,
            "file_end": end,
            "event_id": event["id"],
            "agent": event["agent"],
            "event_type": event["type"],
            "start": event["start"],
            "end": event["end"],
            "species": agent2species[event["agent"]],
            "content": event["content"]
        })
    return rows


if __name__ == "__main__":
    main(sys.argv[1])
