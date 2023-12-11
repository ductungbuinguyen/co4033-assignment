from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

your_GCS_bucket_name = "divvy_data_lake_co4033-assignment"
gcs_credentials_block_name = "divvy-creds"

credentials_block = GcpCredentials(
    service_account_info={
      "type": "service_account",
  "project_id": "co4033-assignment",
  "private_key_id": "b4ee6299dd3b9393dab2e96ff907a802dec3650e",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC2PeXzToNi5nGX\n2N9EXmrnp3KEtaD14vWS42MXR2xFESn8bYtCz9LQK1GZiHiQCTinhFftHzRwl8YS\nrRXEt9ca3aq7gTivLpbQT2dyPjbWxePUbc9ATeZPRTJ7Jp/ZAR7lX5nAhqtxfIDt\nzut9xYmocALboohgX98+100mswTUxF0HijmZS3xWMcLszcOpoxkzz0+vQ+nfD0+T\nBx141jqtMriqo0QUrg3O/U0GPlKu4E2lD5ogxW7jM1QzXgb9L3xourYA6XzFrA2R\nGS4rjWEop3wOvDMoYJ2uLbJLjy5eFw3eQOdVT5qJjFvUAe0xY/JTaHjbnyKCEPAY\n7CTIWV1LAgMBAAECggEAGogzIj5xV8S/uku+CvDJalCGllZFyNvyGiCLcf2EAHTZ\n7VZFdW9W/IJSBE2faoOX1qD97BfO8AAou4Ss1qcCj3bA6euLmnWTSgtFaZcv2gpA\nJn1A3yvNjAeXAeGYWWTrCpnl2JHCrr228j2zK0HHI4QAkS694XS+cSJcqSBcY7Kz\n8FZjCrGUBn60409cySPrO90eyAJh8jrP61YYzwZZe0QGnWk2HLRYMQ+2+8+lHguJ\n6BMONqMWWK8RrhSNLnpqd0tdUhGTWI7wUUEGFQeQZPJkV8bVkF0OQNFcLsbY4SUE\nm8VNfDWpqti3Y7OYaPdTFi1yvcR+QfGGhAx0SwUCAQKBgQDkW1oL8gdeK6xYSmai\njPjgVR4hrCdJ1VsUBZ1/e9e9eMic+7pyYj/12YbJ9VPRvd2AU9MlEi8h1u6kELEk\nY0mJSJa/X0zFPYQ23qUiaMQ6pc6iquA/UvOlKiDqs4BIqRb4TmbquNF8p5HSw+yJ\nrkcIjYEYiU3G4w+p6P3fc3qgsQKBgQDMTXei0hI2IgFRrnO4IxXbSlzld7RTpMPF\nww410c0tviPwtza2WZKYUIvnWpN52T1CIWFZwGvh1r95HqT/tiuvG9WQ0Lh5unbg\nR4YqXUAPtZJW5jTjCpZc4jfKBeAK10HBHRu0vHY7WNP7fkJs59wyPzxHjRRcvH0c\nrscf7QS8uwKBgEuoHHhSsrA3hf0mgUSfsiEY8kezsryajHdguOKw1DR5HPYtzILz\nHD/G1wp4VlHqNsW6f/0LPkRW4m/G+/mTT5zFSPSUjWnXzMMlFs8EqObJYmGwfWVd\nhU2WXJuF5x2rr+mKiDyCCR3kosKSmP6wl+Y0g63uZ69aMSlZASuUJe+xAoGBAJs/\nbt76dT5PfuMcBvFxL+Wwz4DMAurgtKft/jllkV3mNBdykg/lZyF5a6bFzHV2YDeF\neB2kvBYkguG+KLHsM6vgIdzmzpgp2rW5hDljHRAS8P459wxkejhg6vYImuSCCIR/\n4xLnd4zHhmaA3mc1lb1VEjBpA4Uo8oW+RrB7AvOLAoGBAK356Lc3rtMiWE6TabOC\nZ17hZ8km6SyZt/9ph+c2OxNUZYb0jHDXnSI24WRcobUV9lT2H3iC8M+flQXkbrd0\ntskoIrBG02sM9blGKVkCQfgpmLd3ZZLP1uWZ5HZunzNMWgxvsJ5SsUMdu8GR9pZ0\n/XRtUZ6tr7eV6LGZ2JBLOqYy\n-----END PRIVATE KEY-----\n",
  "client_email": "co4033-assignment@co4033-assignment.iam.gserviceaccount.com",
  "client_id": "104399441805543067092",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/co4033-assignment%40co4033-assignment.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
)

credentials_block.save(f"{gcs_credentials_block_name}", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("divvy-creds"),
    bucket="divvy_data_lake_co4033-assignment",
)

bucket_block.save(f"{gcs_credentials_block_name}-bucket", overwrite=True)