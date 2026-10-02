import Foundation

@available(macOS 12.0, *)
extension SQLServerAdministrationClient {
    /// Changes the name the instance reports as `@@SERVERNAME` (`sp_dropserver` / `sp_addserver
    /// 'name', 'local'`), e.g. after the host was renamed or an image was copied. Takes effect when
    /// the service restarts.
    public func renameServer(to name: String) async throws {
        _ = try await client.execute(Self.renameServerSQL(name))
    }

    internal static func renameServerSQL(_ name: String) -> String {
        """
        DECLARE @old sysname = @@SERVERNAME;
        IF @old IS NOT NULL EXEC sp_dropserver @old;
        EXEC sp_addserver N'\(SQLServerSQL.escapeLiteral(name))', 'local';
        """
    }
}
