# syntax=docker/dockerfile:1
#
# Updated dockerfile syntax is required for --mount option
#
# Declares a two-stage build that initially builds all python
# packages and installs them into .local in the first stage.
# Then use another stage to actually create the image without
# temporary files or git by copying .local from first stage
# and prepending it to PATH.
#
#
# Building the image
# ##################
#
# The build needs credentials to access Bitbucket. It uses
# --secret option, which is a feature provided by buildkit.
# The secret is expected to be accessible within the build-
# container via id 'bbcreds'. See buildkit manual on how to
# pass the secret to the build.
#
# Example with a mysecret-file:
#
# $ echo bbhandle:passw0rd > mysecret
# $ DOCKER_BUILDKIT=1 docker build \
#     -t "cdcagg-oai" \
#     --secret id=bbcreds,src=mysecret .
#
#
# Running the container
# #####################
#
# Container needs to expose port 6003 to host machine.
# CDCAGG_DS_URL environment variable should contain an URL
# pointing to the Aggregator Document Store.
# CDCAGG_OPRH_OP_EMAIL_ADMIN environment variable should
# contain admin email, which is displayed when responding
# to the Identify-verb. See OAI-PMH documentation for more
# information.
# CDCAGG_OPRH_OP_BASE_URL environment variable should contain
# the base url for the OAI-PMH endpoint. See OAI-PMH documentation
# for more information.
#
# Example command to start serving the OAI-PMH endpoint:
#
# $ docker run \
#     --name "cdcagg_oai" \
#     -p 5003:6003 \
#     -e "CDCAGG_DS_URL=http://153.1.61.18:5001/v0"
#     -e "CDCAGG_OPRH_OP_EMAIL_ADMIN=admin@example.com"
#     -e "CDCAGG_OPRH_OP_BASE_URL=https://my-oai-endpoint.com"
#     cdcagg-oai

FROM python:3.9-slim as builder

COPY . /docker-build
WORKDIR /docker-build

RUN apt-get update \
  && apt-get install git -y \
  && apt-get clean

# Expects 'bbcreds'-secret.

RUN --mount=type=secret,id=bbcreds \
  BBCREDS=$(cat /run/secrets/bbcreds) \
  pip install --user -r requirements.txt \
  && pip install --user .


# END FIRST STAGE


FROM python:3.9-slim as prod

# Copy built packages from builder image to prod.
# Add them to PATH.

COPY --from=builder /root/.local /root/.local
COPY --from=builder /docker-build/sources_default.yaml .
ENV PATH=/root/.local/bin:$PATH

ENTRYPOINT ["python", "-m", "cdcagg_oai"]
