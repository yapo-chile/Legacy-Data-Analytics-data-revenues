import environ

INI_DW = environ.secrets.INISecrets.from_path_in_env("APP_DW_SECRET")
INI_BLOCKET = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")

@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """
    @environ.config(prefix="DB")
    class BlocketConfig:
        """
        BlocketConfig Class representing the configuration to access
        the database blocket
        """
        host: str = INI_BLOCKET.secret(name="host", default=environ.var())
        port: int = INI_BLOCKET.secret(name="port", default=environ.var())
        name: str = INI_BLOCKET.secret(name="dbname", default=environ.var())
        user: str = INI_BLOCKET.secret(name="user", default=environ.var())
        password: str = INI_BLOCKET.secret(
            name="password", default=environ.var())

    @environ.config(prefix="DW")
    class DWConfig:
        """
        DWConfig Class representing the configuration to access the database
        """
        host: str = INI_DW.secret(name="host", default=environ.var())
        port: int = INI_DW.secret(name="port", default=environ.var())
        name: str = INI_DW.secret(name="dbname", default=environ.var())
        user: str = INI_DW.secret(name="user", default=environ.var())
        password: str = INI_DW.secret(name="password", default=environ.var())


    blocket = environ.group(BlocketConfig)
    db = environ.group(DWConfig)


def getConf():
    return environ.to_config(AppConfig)
