package network

import "net"

// Ensure all bytes in the buffer are sent to the network.
func WriteAll(conn net.Conn, buf []byte) error {
	totalSent := 0
	for totalSent < len(buf) {
		n, err := conn.Write(buf[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}

// Ensure to read the required # of bytes from conn
func ReadAll(conn net.Conn, buf []byte, n uint64) error {
	totalRead := uint64(0)
	for totalRead < n {
		read, err := conn.Read(buf[totalRead:n])
		if err != nil {
			return err
		}
		totalRead += uint64(read)
	}
	return nil
}
