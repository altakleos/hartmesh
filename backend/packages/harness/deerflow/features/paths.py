"""Host-only admitted compatibility paths, never browser-selected roots."""

from deerflow.config.paths import Paths
from deerflow.spaces.filesystem import _path


class FeaturePaths(Paths):
    resource_shelf = True

    def __init__(self, base, user_id, volumes):
        self.__dict__.update(base.__dict__)
        self._feature_user = user_id
        self._feature_volumes = dict(volumes)

    def _volume(self, namespace, user_id=None):
        if user_id is not None and user_id != self._feature_user:
            raise PermissionError("Feature paths cannot select another owner")
        if namespace not in self._feature_volumes:
            raise PermissionError("This operation has no admitted feature resource")
        return self._feature_volumes[namespace]

    def user_files_dir(self, user_id):
        return self._volume("hm.my-files", user_id).data_path

    def ensure_user_files_dir(self, user_id):
        return self.user_files_dir(user_id)

    def user_files_control_dir(self, user_id):
        return self._volume("hm.my-files", user_id).control_path

    def shared_dir(self):
        return self._volume("hm.shared").data_path

    def ensure_shared_dir(self):
        return self.shared_dir()

    def shared_control_dir(self):
        return self._volume("hm.shared").control_path

    def user_projects_dir(self, user_id):
        return self._volume("hm.projects", user_id).data_path

    def project_staging_dir(self, user_id, project_id):
        _path(project_id)
        return self._volume("hm.projects", user_id).control_path / "project-staging" / project_id

    def project_conversion_dir(self, user_id):
        return self._volume("hm.projects", user_id).control_path / "project-conversion"
