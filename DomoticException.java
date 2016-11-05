package domotic.eedomus;

public class DomoticException extends Exception {
	
    private static final long serialVersionUID = 1997753363232807009L;

    public	DomoticException()
	{
	}
	
    public	DomoticException(String message)
	{
		super(message);
	}
	
    public	DomoticException(Throwable cause)
    {
        super(cause);
    }

    public	DomoticException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
