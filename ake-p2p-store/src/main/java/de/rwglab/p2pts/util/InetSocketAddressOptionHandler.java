package de.rwglab.p2pts.util;

import com.google.common.primitives.Ints;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

import java.net.InetSocketAddress;

public class InetSocketAddressOptionHandler extends OptionHandler<InetSocketAddress> {

	public InetSocketAddressOptionHandler(CmdLineParser parser, OptionDef option,
										  Setter<? super InetSocketAddress> setter) {
		super(parser, option, setter);
	}

	@Override
	public int parseArguments(final Parameters parameters) throws CmdLineException {
		String parameter = parameters.getParameter(0);
		String[] split = parameter.split(":");
		if (split.length < 2 || Ints.tryParse(split[1]) == null) {
			throw new CmdLineException(owner,
					"The parameter " + option.toString() + " must be of the form 'hostname:port'"
			);
		}
		String host = split[0];
		Integer port = Ints.tryParse(split[1]);
		setter.addValue(new InetSocketAddress(host, port));
		return 1;
	}

	@Override
	public String getDefaultMetaVariable() {
		return "HOSTNAME:PORT";
	}

}
