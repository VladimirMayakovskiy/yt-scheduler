from scheduler import Job, Scheduler, run_job
import yt.wrapper as yt

def scheduler(args):
    yt_client = yt.YtClient(proxy=args.yt_proxy)

    job_runner = Scheduler(job = Job(), yt_client=yt_client)
    run_job(job=job_runner.job, execute_callable=job_runner._execute, yt_client=yt_client)