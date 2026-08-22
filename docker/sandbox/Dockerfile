ARG BASE_IMAGE=enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox@sha256:6328d7fd2f0ff0b4c147c3d05b3df1ce331f4a482eb6e550ecd64ed1fcf906e7
FROM ${BASE_IMAGE}

USER 0

# Stage 1: pre-seed the runtime user, directories, and ownership.
RUN set -eux; \
    groupadd --gid 1000 gem; useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home gem; \
    mkdir -p /home/gem/Downloads /var/log/gem /var/log/nginx /run/gem /run/user/1000 \
             /var/lib/nginx /var/lib/aio-sandbox/nginx/body /var/lib/aio-sandbox/nginx/proxy \
             /var/lib/aio-sandbox/nginx/fastcgi /var/lib/aio-sandbox/nginx/scgi \
             /var/lib/aio-sandbox/nginx/uwsgi /etc/browser/policies/managed /opt/aio /var/lock; \
    chown -R 1000:1000 /home/gem /var/log/gem /var/log/nginx /run /var/lib/nginx \
                       /var/lib/aio-sandbox /opt/gem /opt/aio /opt/jupyter /opt/skills /etc/browser; \
    chmod 1777 /var/log/gem /var/lib/nginx /var/lock

# Stage 2: assert, then substitute only the vendor operations that restricted forbids.
# Every grep fails the build if the pinned base moves the source line.
RUN set -eux; \
    G=/opt/gem/gem.sh; S=/opt/gem/supervisord.conf; N=/opt/gem/supervisord/nginx.conf; \
    B=/opt/gem/browser-supervisor.py; \
    grep -q 'chown "root:${USER}" "${RUN_DIR}"'      $G; sed -i 's|chown "root:${USER}" "${RUN_DIR}"|chown "${USER}:${USER}" "${RUN_DIR}"|'       $G; \
    grep -q 'chown "root:${USER}" "${tmp_config}"'   $G; sed -i 's|chown "root:${USER}" "${tmp_config}"|chown "${USER}:${USER}" "${tmp_config}"|' $G; \
    grep -q '^chown nobody /var/lib/nginx'           $G; sed -i 's|^chown nobody /var/lib/nginx|chown $USER:$USER /var/lib/nginx|'                $G; \
    grep -q 'chown nobody:root "${NGINX_RUNTIME_DIR}"' $G; sed -i 's|chown nobody:root "${NGINX_RUNTIME_DIR}"|chown $USER:$USER "${NGINX_RUNTIME_DIR}"|' $G; \
    grep -q 'chown nobody:root "${NGINX_RUNTIME_DIR}/${temp_dir}"' $G; sed -i 's|chown nobody:root "${NGINX_RUNTIME_DIR}/${temp_dir}"|chown $USER:$USER "${NGINX_RUNTIME_DIR}/${temp_dir}"|' $G; \
    grep -q '^user=root'                             $S; sed -i 's|^user=root|user=%(ENV_USER)s|'                                                 $S; \
    grep -q '^chown=root:%(ENV_USER)s'               $S; sed -i 's|^chown=root:%(ENV_USER)s|chown=%(ENV_USER)s:%(ENV_USER)s|'                     $S; \
    grep -q '^user=root'                             $N; sed -i 's|^user=root|user=%(ENV_USER)s|'                                                 $N; \
    grep -q 'os.chown(tmp_path, 0, gid)'             $B; sed -i 's|os.chown(tmp_path, 0, gid)|os.chown(tmp_path, os.getuid(), gid)|'              $B; \
    grep -q 'os.initgroups(username, gid)'           $B; sed -i 's|os.initgroups(username, gid)|(os.initgroups(username, gid) if os.getuid() == 0 else None)|' $B; \
    test "$(grep -cE 'chown "?(nobody|root)' $G)" = 1

# Stage 3: replace su only for its same-uid, setgroups-free path.
COPY --chmod=0755 su-shim.sh /usr/local/bin/su

ENV BROWSER_NO_SANDBOX=--no-sandbox
USER 1000:1000
