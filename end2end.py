import multiprocessing as mp
from mapbox import Uploader
from os.path import dirname as up
import time
import argparse
import subprocess
import os


temp_merge = []  # checking subprocess thread of merge
temp_delete = []  # checking subprocess thread of delete


def get_args():
    parser = argparse.ArgumentParser(description="Converting into mbtiles")
    parser.add_argument("--dir", type=str, help="GeoJSON Directory")

    return parser.parse_args()


def arrange_data(input_dir):
    try:
        print("REARRANGING FILES IN FOLDER ...")
        input_abs_dir = os.path.abspath(input_dir)
        get_metadata = os.listdir(input_abs_dir)[0].split("_")

        parent_dir = ["CB", "TURB", "ICE"]
        children_dir = [
            "06_{}_{}".format(get_metadata[3], get_metadata[4]),
            "09_{}_{}".format(get_metadata[3], get_metadata[4]),
            "12_{}_{}".format(get_metadata[3], get_metadata[4]),
            "15_{}_{}".format(get_metadata[3], get_metadata[4]),
            "18_{}_{}".format(get_metadata[3], get_metadata[4]),
            "21_{}_{}".format(get_metadata[3], get_metadata[4]),
            "24_{}_{}".format(get_metadata[3], get_metadata[4]),
            "27_{}_{}".format(get_metadata[3], get_metadata[4]),
            "30_{}_{}".format(get_metadata[3], get_metadata[4]),
            "33_{}_{}".format(get_metadata[3], get_metadata[4]),
            "36_{}_{}".format(get_metadata[3], get_metadata[4]),
        ]

        for parent in parent_dir:
            base_dir = os.path.join(input_abs_dir, parent)
            if not os.path.isdir(base_dir):
                os.mkdir(base_dir)

            for child in children_dir:
                child_dir = os.path.join(
                    base_dir, os.path.basename(base_dir) + "_" + child
                )
                if not os.path.isdir(child_dir):
                    os.mkdir(child_dir)

                for file in os.listdir(input_abs_dir):
                    if file.endswith(".geojson"):
                        temp_arr = file.split("_")
                        if (
                            parent in file
                            and temp_arr[2] in file
                            and temp_arr[2] == child.split("_")[0]
                        ):
                            os.rename(
                                os.path.join(input_abs_dir, file),
                                os.path.join(child_dir, file),
                            )

        print("REARRANGING FILES OPERATION COMPLETED!")
        print(
            "******************************************************************************************"
        )
    except:
        print("SOMETHING WENT WRONG WHILE REARRANGING FILES IN FOLDER")


def geojson_to_mbtiles(geojson_dir):
    geojson_abs_path = os.path.abspath(geojson_dir)
    # os.chdir(geojson_abs_path)
    mbtiles_dir = os.path.join(up(up(up(geojson_abs_path))), "MBTILES")
    mbtiles_dir_content = os.path.join(mbtiles_dir, os.path.basename(geojson_abs_path))
    if not os.path.isdir(mbtiles_dir):
        os.mkdir(mbtiles_dir)
    if not os.path.isdir(mbtiles_dir_content):
        os.mkdir(mbtiles_dir_content)

    for file in os.listdir(geojson_abs_path):
        if file.endswith(".geojson"):
            output_file_name = file.split(".")[0] + ".mbtiles"
            output_file = os.path.join(mbtiles_dir_content, output_file_name)
            subprocess.run(
                [
                    "tippecanoe",
                    "-z8",
                    "-o",
                    output_file,
                    os.path.join(geojson_abs_path, file),
                    "--force",
                ]
            )


def geojson_to_mbtiles_mp(dir_path):
    try:
        # Convert into mbtiles
        print("CONVERTING INTO MBTILES ...")
        print("BE PATIENT ..., THIS OPERATION TAKES TIME")
        input_dir = os.path.abspath(dir_path)
        pool = mp.Pool(mp.cpu_count())
        for root_folder in os.listdir(input_dir):
            root_abs_path = os.path.join(input_dir, root_folder)
            # pool = mp.Pool(mp.cpu_count())
            pool.map(
                geojson_to_mbtiles,
                [
                    os.path.join(root_abs_path, folder)
                    for folder in os.listdir(root_abs_path)
                ],
            )

        pool.close()
        pool.join()
        print("CONVERTING INTO MBTILES OPERATION COMPLETED!")
        print(
            "******************************************************************************************"
        )
    except:
        print("SOMETHING WENT WRONG WHILE CONVERTING INTO MBTILES")


def merge_tiles(tiles_dir):
    tiles_abs_dir = os.path.abspath(tiles_dir)
    merged_tiles_dir = os.path.join(up(up(tiles_dir)), "MERGEDTILES")

    output_file_name = "FTSIGWX25RU_" + os.path.basename(tiles_abs_dir) + ".mbtiles"
    output_file = os.path.join(merged_tiles_dir, output_file_name)
    cmd = "tile-join -o {} {} --force".format(
        output_file, os.path.join(tiles_abs_dir, "*.mbtiles")
    )
    return subprocess.Popen(cmd, shell=True)


def merge_tiles_mp(dir_path):
    try:
        print("MERGING MBTILES ...")
        input_dir = os.path.abspath(dir_path)
        mbtiles_dir = os.path.join(up(input_dir), "MBTILES")
        merged_tiles_dir = os.path.join(up(input_dir), "MERGEDTILES")
        if not os.path.isdir(merged_tiles_dir):
            os.mkdir(merged_tiles_dir)

        for folder in os.listdir(mbtiles_dir):
            temp_merge.append(merge_tiles(os.path.join(mbtiles_dir, folder)))

        # Checking for subprocess thread is terminated or not
        all_dead = False
        temp2 = [0] * len(temp_merge)
        while not all_dead:
            for index, p in enumerate(temp_merge):
                if p.poll() is None:
                    time.sleep(1)
                else:
                    temp2[index] = 1

            all_dead = all(el == 1 for el in temp2)

        print("MERGING MBTILES OPERATION COMPLETED!")
        print("FINAL OUTPUTS ARE THERE AT: ", merged_tiles_dir)
        print(
            "******************************************************************************************"
        )
    except:
        print("SOMETHING WENT WRONG WHILE MERGING MBTILES")


def get_all_tilesets():
    try:
        access_token = "sk.eyJ1IjoicmFodWxzZHMiLCJhIjoiY2w3c3JoN3loMGJ3ZjN2bGVqcTU0dXRjciJ9.9Ka6FvkwaI5GbvoAOtiZYA"
        temp_file = os.path.join(os.getcwd(), "log.txt")
        cmd = "tilesets list -t {} rahulsds > {}".format(access_token, temp_file)
        subprocess.Popen(cmd, shell=True)

        return temp_file
    except:
        print(
            "SOMETHING WENT WRONG WHILE READING UPLOADED TILES IN MAPBOX: CHECK MAPBOX_ACCEESS_TOKEN"
        )


def delete_tileset(id):
    access_token = "sk.eyJ1IjoicmFodWxzZHMiLCJhIjoiY2w3c3JoN3loMGJ3ZjN2bGVqcTU0dXRjciJ9.9Ka6FvkwaI5GbvoAOtiZYA"
    cmd = "tilesets delete -t {} {} -f".format(access_token, id)
    subprocess.Popen(cmd, shell=True)
    # subprocess.run(["tilesets", "delete", id, "-f"])
    # print("{} IS DELETED FROM MAPBOX".format(id))


def delete_tileset_from_mapbox():
    try:
        print("DELETING 0.25 DEGREE OLD TILESETS FROM MAPBOX")
        tileset_id_file = get_all_tilesets()
        with open(tileset_id_file, "r") as file:
            for line in file:
                if "FTSIGWX25RU_" in line.strip():
                    delete_tileset(line.strip())

        # # Checking for subprocess thread is terminated or not
        # all_dead = False
        # temp2 = [0] * len(temp_delete)
        # while not all_dead:
        #     for index, p in enumerate(temp_delete):
        #         if p.poll() is None:
        #             time.sleep(1)
        #         else:
        #             temp2[index] = 1
        #
        #     all_dead = all(el == 1 for el in temp2)

        print("DELETING TILESETS FROM MAPBOX OPERATION COMPLETED!")
        print(
            "******************************************************************************************"
        )
    except:
        print("SOMETHING WENT WRONG WHILE DELETING TILESETS FROM MAPBOX")


def upload_to_mapbox(file_to_upload):
    output_file_name = os.path.basename(file_to_upload).split(".")[0]
    valid_tileset_name = output_file_name.replace(" ", "_")
    valid_tileset_id = valid_tileset_name

    service = Uploader(
        access_token="sk.eyJ1IjoicmFodWxzZHMiLCJhIjoiY2w3c3JoN3loMGJ3ZjN2bGVqcTU0dXRjciJ9.9Ka6FvkwaI5GbvoAOtiZYA"
    )
    with open(file_to_upload, "rb") as src:
        upload_resp = service.upload(
            src,
            name=valid_tileset_name
            if len(valid_tileset_name) == 32
            else valid_tileset_name[0:32],
            tileset=valid_tileset_id,
        )

    return upload_resp


def upload_to_mapbox_mp(dir_path):
    try:
        print("UPLOADING MERGED MBTILES TO MAPBOX ... BE PATIENT")
        input_dir = os.path.abspath(dir_path)
        merged_tiles_dir = os.path.join(up(input_dir), "MERGEDTILES")
        pool = mp.Pool(mp.cpu_count())
        pool.map(
            upload_to_mapbox,
            [
                os.path.join(os.path.abspath(merged_tiles_dir), file)
                for file in os.listdir(merged_tiles_dir)
            ],
        )
        pool.close()
        pool.join()
        print("UPLOADING MERGED MBTILES TO MAPBOX OPERATIONS ARE COMPLETED!")
        print(
            "******************************************************************************************"
        )
        print("THANK YOU!")
    except:
        print("SOMETHING WENT WRONG WHILE UPLOADING TILESETS TO MAPBOX")


def main(dir_path):
    input_dir = os.path.abspath(dir_path)
    arrange_data(input_dir)
    geojson_to_mbtiles_mp(input_dir)
    merge_tiles_mp(input_dir)
    delete_tileset_from_mapbox()
    upload_to_mapbox_mp(input_dir)


if __name__ == "__main__":
    args = get_args()
    main(args.dir)
