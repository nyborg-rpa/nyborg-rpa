import requests
from tqdm.auto import tqdm


def latest_commit_hash(
    *,
    repository: str,
    path: str,
    sha: str = "main",
) -> str:
    """
    Get the latest commit hash (SHA) for a specific file in a GitHub repository.

    Args:
        repository: The GitHub repository in the format "owner/repo".
        path: The file path within the repository.
        sha: The branch or commit SHA to start from. Defaults to "main".
    """

    owner, repo = repository.split("/")
    url = f"https://api.github.com/repos/{owner}/{repo}/commits?path={path}&sha={sha}"
    resp = requests.get(url)
    resp.raise_for_status()

    commits = resp.json()
    if not commits:
        raise FileNotFoundError(f"No commits found for file at {url=!r}")

    return commits[0]["sha"]
