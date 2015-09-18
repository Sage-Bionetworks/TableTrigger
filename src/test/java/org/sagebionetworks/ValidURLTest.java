package org.sagebionetworks;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.sagebionetworks.TableTrigger.isValidDockerReference;

import org.junit.Test;

public class ValidURLTest {

	@Test
	public void testValidURL() throws Exception {
		// valid
		assertTrue(isValidDockerReference("/user_name/model_name"));

		// username not right
		assertFalse(isValidDockerReference("//model_name"));
		assertFalse(isValidDockerReference("/user#name/model_name"));

		// repo/model name not right
		assertFalse(isValidDockerReference("/user_name/"));
		assertFalse(isValidDockerReference("/user_name/model&name"));
	}

}
