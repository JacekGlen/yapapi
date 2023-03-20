#!/usr/bin/env python3
import pathlib
import random
import sys
from datetime import datetime, timedelta

from yapapi import Golem, Task, WorkContext
from yapapi.payload import vm
from yapapi.rest.activity import BatchTimeoutError

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import (
    TEXT_COLOR_CYAN,
    TEXT_COLOR_DEFAULT,
    TEXT_COLOR_MAGENTA,
    TEXT_COLOR_RED,
    build_parser,
    format_usage,
    print_env_info,
    run_golem_example,
)

class RenderPart():
    def __init__(self, id, start_frame, end_frame):
        self.id = id
        self.start_frame = start_frame
        self.end_frame = end_frame

async def main(
    subnet_tag, min_cpu_threads, payment_driver=None, payment_network=None, show_usage=False
):
    package = await vm.repo(
        image_hash="3c744aa70415ea82c929a09a3faf81b2711eda12447f3e025a3f9745",
        # only run on provider nodes that have more than 0.5gb of RAM available
        min_mem_gib=0.5,
        # only run on provider nodes that have more than 2gb of storage space available
        min_storage_gib=2.0,
        # only run on provider nodes which a certain number of CPU threads (logical CPU cores)
        #  available
        min_cpu_threads=min_cpu_threads,
    )

    async def worker(ctx: WorkContext, tasks):
        script_dir = pathlib.Path(__file__).resolve().parent
        scene_path = str(script_dir / "data" / "Eiffel.blend")

        # Set timeout for the first script executed on the provider. Usually, 30 seconds
        # should be more than enough for computing a single frame of the provided scene,
        # however a provider may require more time for the first task if it needs to download
        # the VM image first. Once downloaded, the VM image will be cached and other tasks that use
        # that image will be computed faster.
        script = ctx.new_script(timeout=timedelta(minutes=30))
        script.upload_file(scene_path, "/golem/work/scene.blend")

        async for task in tasks:
            # frame = task.data
            renderPart : RenderPart = task.data
            # crops = [{"outfilebasename": "out", "borders_x": [0.0, 1.0], "borders_y": [0.0, 1.0]}]
            script.upload_json(
                {
                    "stills": False,
                    "scene_file": "/golem/work/scene.blend",
                    "output_file": "rendered",
                    "resolution": [1000, 1000],
                    "samples": 8,
                    "frames": [renderPart.start_frame, renderPart.end_frame],
                    "replacement_texts": [
                        {
                            "text": "Jacek",
                            "object": "Text",
                            "data_path": "data.body"
                        }
                    ],
                    "replacement_images": []
                },
                "/golem/work/params.json",
            )

            script.run("/usr/local/bin/python", "/golem/entrypoint/render_entrypoint.py", "--params", "/golem/work/params.json")
            randomId = random.randint(0, 1000)
            output_file = f"output_{renderPart.id}.mp4"
            script.download_file(f"/golem/output/rendered0{renderPart.start_frame}-0{renderPart.end_frame}.mp4", output_file)
            stats_results = script.run("/bin/sh", "-c", "date")
            try:
                yield script

                stats = (await stats_results).stdout.strip()

                print(stats)
                # TODO: Check if job results are valid
                # and reject by: task.reject_task(reason = 'invalid file')
                task.accept_result(result="stats")
            except BatchTimeoutError:
                print(
                    f"{TEXT_COLOR_RED}"
                    f"Task {task} timed out on {ctx.provider_name}, time: {task.running_time}"
                    f"{TEXT_COLOR_DEFAULT}"
                )
                raise

            # reinitialize the script which we send to the engine to compute subsequent frames
            script = ctx.new_script(timeout=timedelta(minutes=1))

            if show_usage:
                raw_state = await ctx.get_raw_state()
                usage = format_usage(await ctx.get_usage())
                cost = await ctx.get_cost()
                print(
                    f"{TEXT_COLOR_MAGENTA}"
                    f" --- {ctx.provider_name} STATE: {raw_state}\n"
                    f" --- {ctx.provider_name} USAGE: {usage}\n"
                    f" --- {ctx.provider_name}  COST: {cost}"
                    f"{TEXT_COLOR_DEFAULT}"
                )

    # Iterator over the frame indices that we want to render
    frames: range = range(0, 10, 10)
    # Worst-case overhead, in minutes, for initialization (negotiation, file transfer etc.)
    # TODO: make this dynamic, e.g. depending on the size of files to transfer
    init_overhead = 3
    # Providers will not accept work if the timeout is outside of the [5 min, 30min] range.
    # We increase the lower bound to 6 min to account for the time needed for our demand to
    # reach the providers.
    min_timeout, max_timeout = 6, 30

    parts = [
        RenderPart(1, 843, 852),
        RenderPart(2, 853, 862),
        RenderPart(3, 863, 872),
        RenderPart(4, 873, 882),
        RenderPart(5, 883, 892),
        RenderPart(6, 893, 900),
    ]

    timeout = timedelta(minutes=30)

    async with Golem(
        budget=10.0,
        subnet_tag=subnet_tag,
        payment_driver=payment_driver,
        payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        num_tasks = 0
        start_time = datetime.now()

        completed_tasks = golem.execute_tasks(
            worker,
            [Task(data=part) for part in parts],
            payload=package,
            max_workers=3,
            timeout=timeout,
        )
        async for task in completed_tasks:
            num_tasks += 1
            print(
                f"{TEXT_COLOR_CYAN}"
                f"Task computed: {task}, result: {task.result}, time: {task.running_time}"
                f"{TEXT_COLOR_DEFAULT}"
            )

        print(
            f"{TEXT_COLOR_CYAN}"
            f"{num_tasks} tasks computed, total time: {datetime.now() - start_time}"
            f"{TEXT_COLOR_DEFAULT}"
        )


if __name__ == "__main__":
    parser = build_parser("Render a Blender scene")
    parser.add_argument("--show-usage", action="store_true",
                        help="show activity usage and cost")
    parser.add_argument(
        "--min-cpu-threads",
        type=int,
        default=1,
        help="require the provider nodes to have at least this number of available CPU threads",
    )
    now = datetime.now().strftime("%Y-%m-%d_%H.%M.%S")
    parser.set_defaults(log_file=f"blender-yapapi-{now}.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            min_cpu_threads=args.min_cpu_threads,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
            show_usage=args.show_usage,
        ),
        log_file=args.log_file,
    )

