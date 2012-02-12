package de.rwglab.p2pts;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import cx.ath.troja.chordless.dhash.DHash;
import de.rwglab.p2pts.util.Log4JTypeListener;
import org.eclipse.jetty.servlet.DefaultServlet;

public class TripleStoreModule extends JerseyServletModule {

	private final TripleStoreConfig config;

	private final TripleStore tripleStore;

	private final DHashService dHashService;

	public TripleStoreModule(final TripleStoreConfig config, final TripleStore tripleStore,
							 final DHashService dHashService) {

		this.config = config;
		this.tripleStore = tripleStore;
		this.dHashService = dHashService;
	}

	@Override
	protected void configureServlets() {

		bind(TripleStoreConfig.class).toInstance(config);
		bind(TripleStore.class).toInstance(tripleStore);
		bind(DHashService.class).toInstance(dHashService);
		bind(DHash.class).toInstance(dHashService.getDhash());

		bind(TripleStoreRestResource.class);
		bind(DefaultServlet.class).in(Singleton.class);

		bindListener(Matchers.any(), new Log4JTypeListener());

		serve("/rest*").with(GuiceContainer.class, ImmutableMap.of(JSONConfiguration.FEATURE_POJO_MAPPING, "true"));
		serve("/*").with(DefaultServlet.class, ImmutableMap.of(
				"resourceBase", this.getClass().getClassLoader().getResource("webapp").toExternalForm(),
				"maxCacheSize", "0"
		));
	}
}
