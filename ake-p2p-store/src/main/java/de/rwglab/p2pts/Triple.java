package de.rwglab.p2pts;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@XmlRootElement
public class Triple implements Serializable {

	public String subject;

	public String predicate;

	public String object;

	public Triple() {
	}

	public Triple(String subject, String predicate, String object) {

		checkArgument((subject != null && predicate != null && object != null) ||
				(subject == null && predicate != null && object != null) ||
				(subject != null && predicate == null && object != null) ||
				(subject != null && predicate != null && object == null)
		);

		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
	}

	public List<String> getTableRow() {
		List<String> row = new ArrayList<String>();
		row.add(subject);
		row.add(predicate);
		row.add(object);
		return row;
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		final Triple triple = (Triple) o;

		if (object != null ? !object.equals(triple.object) : triple.object != null) {
			return false;
		}
		if (predicate != null ? !predicate.equals(triple.predicate) : triple.predicate != null) {
			return false;
		}
		if (subject != null ? !subject.equals(triple.subject) : triple.subject != null) {
			return false;
		}

		return true;
	}

	@Override
	public int hashCode() {
		int result = subject != null ? subject.hashCode() : 0;
		result = 31 * result + (predicate != null ? predicate.hashCode() : 0);
		result = 31 * result + (object != null ? object.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Triple{" +
				"subject='" + subject + '\'' +
				", predicate='" + predicate + '\'' +
				", object='" + object + '\'' +
				'}';
	}
}
